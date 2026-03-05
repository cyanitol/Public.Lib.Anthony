// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package planner

import (
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
)

// CTEContext manages Common Table Expressions during query planning.
// It handles both non-recursive and recursive CTEs, tracking their
// definitions, dependencies, and materialization status.
type CTEContext struct {
	// CTEs maps CTE names to their definitions
	CTEs map[string]*CTEDefinition

	// IsRecursive indicates if the WITH clause uses RECURSIVE
	IsRecursive bool

	// MaterializedCTEs tracks which CTEs have been materialized
	MaterializedCTEs map[string]*MaterializedCTE

	// CTEOrder tracks the order in which CTEs should be evaluated
	CTEOrder []string
}

// CTEDefinition represents a single CTE definition.
type CTEDefinition struct {
	// Name is the CTE name
	Name string

	// Columns are the explicit column names (if specified)
	Columns []string

	// Select is the SELECT statement defining the CTE
	Select *parser.SelectStmt

	// IsRecursive indicates if this CTE is recursive
	IsRecursive bool

	// DependsOn lists other CTEs this CTE references
	DependsOn []string

	// Level indicates the dependency level (for topological sort)
	Level int

	// EstimatedRows is the estimated number of rows
	EstimatedRows LogEst

	// TableInfo represents this CTE as a virtual table
	TableInfo *TableInfo
}

// MaterializedCTE represents a CTE that has been materialized.
type MaterializedCTE struct {
	// Name is the CTE name
	Name string

	// TempTable is the name of the temporary table
	TempTable string

	// Columns are the column definitions
	Columns []ColumnInfo

	// RowCount is the actual row count after materialization
	RowCount int64

	// IsRecursive indicates if this was a recursive CTE
	IsRecursive bool

	// Iterations tracks recursive iterations (for recursive CTEs)
	Iterations int
}

// NewCTEContext creates a new CTE context from a WITH clause.
func NewCTEContext(withClause *parser.WithClause) (*CTEContext, error) {
	if withClause == nil {
		return nil, nil
	}

	ctx := &CTEContext{
		CTEs:             make(map[string]*CTEDefinition),
		IsRecursive:      withClause.Recursive,
		MaterializedCTEs: make(map[string]*MaterializedCTE),
		CTEOrder:         make([]string, 0),
	}

	// Parse each CTE definition
	for _, cte := range withClause.CTEs {
		def := &CTEDefinition{
			Name:          cte.Name,
			Columns:       cte.Columns,
			Select:        cte.Select,
			IsRecursive:   withClause.Recursive && ctx.checkIfRecursive(&cte),
			DependsOn:     make([]string, 0),
			Level:         0,
			EstimatedRows: NewLogEst(100), // Default estimate
		}

		// Analyze dependencies
		deps := ctx.findCTEDependencies(cte.Select)
		def.DependsOn = deps

		ctx.CTEs[cte.Name] = def
	}

	// Build dependency order
	if err := ctx.buildDependencyOrder(); err != nil {
		return nil, err
	}

	return ctx, nil
}

// checkIfRecursive determines if a CTE is recursive.
// A CTE is recursive if it references itself in its definition.
func (ctx *CTEContext) checkIfRecursive(cte *parser.CTE) bool {
	if !ctx.IsRecursive {
		return false
	}

	// Check if the CTE references itself
	return ctx.selectReferencesTable(cte.Select, cte.Name)
}

// selectReferencesTable checks if a SELECT statement references a table name.
func (ctx *CTEContext) selectReferencesTable(sel *parser.SelectStmt, tableName string) bool {
	if sel == nil {
		return false
	}

	// Check FROM clause
	if ctx.fromClauseReferencesTable(sel.From, tableName) {
		return true
	}

	// Check UNION/EXCEPT/INTERSECT compound queries
	if ctx.compoundReferencesTable(sel.Compound, tableName) {
		return true
	}

	return false
}

// fromClauseReferencesTable checks if a FROM clause references a table name.
func (ctx *CTEContext) fromClauseReferencesTable(from *parser.FromClause, tableName string) bool {
	if from == nil {
		return false
	}

	// Check tables in FROM clause
	if ctx.tablesReferenceTable(from.Tables, tableName) {
		return true
	}

	// Check JOINs
	if ctx.joinsReferenceTable(from.Joins, tableName) {
		return true
	}

	return false
}

// tablesReferenceTable checks if a slice of tables references a table name.
func (ctx *CTEContext) tablesReferenceTable(tables []parser.TableOrSubquery, tableName string) bool {
	for _, table := range tables {
		if table.TableName == tableName {
			return true
		}
		// Check subqueries
		if table.Subquery != nil && ctx.selectReferencesTable(table.Subquery, tableName) {
			return true
		}
	}
	return false
}

// joinsReferenceTable checks if a slice of joins references a table name.
func (ctx *CTEContext) joinsReferenceTable(joins []parser.JoinClause, tableName string) bool {
	for _, join := range joins {
		if join.Table.TableName == tableName {
			return true
		}
		if join.Table.Subquery != nil && ctx.selectReferencesTable(join.Table.Subquery, tableName) {
			return true
		}
	}
	return false
}

// compoundReferencesTable checks if a compound query references a table name.
func (ctx *CTEContext) compoundReferencesTable(compound *parser.CompoundSelect, tableName string) bool {
	if compound == nil {
		return false
	}

	if ctx.selectReferencesTable(compound.Left, tableName) {
		return true
	}
	if ctx.selectReferencesTable(compound.Right, tableName) {
		return true
	}

	return false
}

// findCTEDependencies finds all CTE dependencies in a SELECT statement.
func (ctx *CTEContext) findCTEDependencies(sel *parser.SelectStmt) []string {
	deps := make(map[string]bool)
	ctx.collectCTEReferences(sel, deps)

	result := make([]string, 0, len(deps))
	for dep := range deps {
		result = append(result, dep)
	}
	return result
}

// collectCTEReferences recursively collects all CTE references.
func (ctx *CTEContext) collectCTEReferences(sel *parser.SelectStmt, deps map[string]bool) {
	if sel == nil {
		return
	}

	ctx.collectFromClauseRefs(sel, deps)
	ctx.collectWhereHavingRefs(sel, deps)
	ctx.collectCompoundRefs(sel, deps)
}

// collectFromClauseRefs collects CTE references from FROM clause.
func (ctx *CTEContext) collectFromClauseRefs(sel *parser.SelectStmt, deps map[string]bool) {
	if sel.From == nil {
		return
	}

	for _, table := range sel.From.Tables {
		ctx.processTableReference(table.TableName, table.Subquery, deps)
	}

	for _, join := range sel.From.Joins {
		ctx.processTableReference(join.Table.TableName, join.Table.Subquery, deps)
	}
}

// processTableReference processes a table reference for CTE dependencies.
func (ctx *CTEContext) processTableReference(tableName string, subquery *parser.SelectStmt, deps map[string]bool) {
	if _, exists := ctx.CTEs[tableName]; exists {
		deps[tableName] = true
	}
	if subquery != nil {
		ctx.collectCTEReferences(subquery, deps)
	}
}

// collectWhereHavingRefs collects CTE references from WHERE and HAVING clauses.
func (ctx *CTEContext) collectWhereHavingRefs(sel *parser.SelectStmt, deps map[string]bool) {
	if sel.Where != nil {
		ctx.collectCTEReferencesInExpr(sel.Where, deps)
	}
	if sel.Having != nil {
		ctx.collectCTEReferencesInExpr(sel.Having, deps)
	}
}

// collectCompoundRefs collects CTE references from compound queries.
func (ctx *CTEContext) collectCompoundRefs(sel *parser.SelectStmt, deps map[string]bool) {
	if sel.Compound != nil {
		ctx.collectCTEReferences(sel.Compound.Left, deps)
		ctx.collectCTEReferences(sel.Compound.Right, deps)
	}
}

// collectCTEReferencesInExpr recursively collects CTE references from expressions.
func (ctx *CTEContext) collectCTEReferencesInExpr(expr parser.Expression, deps map[string]bool) {
	if expr == nil {
		return
	}

	switch e := expr.(type) {
	case *parser.SubqueryExpr:
		ctx.handleSubqueryExpr(e, deps)
	case *parser.InExpr:
		ctx.handleInExpr(e, deps)
	case *parser.BinaryExpr:
		ctx.handleBinaryExpr(e, deps)
	case *parser.CaseExpr:
		ctx.handleCaseExpr(e, deps)
	case *parser.BetweenExpr:
		ctx.handleBetweenExpr(e, deps)
	case *parser.FunctionExpr:
		ctx.handleFunctionExpr(e, deps)
	default:
		ctx.handleSimpleWrapperExpr(e, deps)
	}
}

// handleSimpleWrapperExpr handles expression types that simply wrap another expression
func (ctx *CTEContext) handleSimpleWrapperExpr(expr parser.Expression, deps map[string]bool) {
	switch e := expr.(type) {
	case *parser.UnaryExpr:
		ctx.collectCTEReferencesInExpr(e.Expr, deps)
	case *parser.ParenExpr:
		ctx.collectCTEReferencesInExpr(e.Expr, deps)
	case *parser.CastExpr:
		ctx.collectCTEReferencesInExpr(e.Expr, deps)
	case *parser.CollateExpr:
		ctx.collectCTEReferencesInExpr(e.Expr, deps)
	}
}

// handleSubqueryExpr handles subquery expressions.
func (ctx *CTEContext) handleSubqueryExpr(e *parser.SubqueryExpr, deps map[string]bool) {
	if e.Select != nil {
		ctx.collectCTEReferences(e.Select, deps)
	}
}

// handleInExpr handles IN expressions.
func (ctx *CTEContext) handleInExpr(e *parser.InExpr, deps map[string]bool) {
	if e.Select != nil {
		ctx.collectCTEReferences(e.Select, deps)
	}
	ctx.collectCTEReferencesInExpr(e.Expr, deps)
	for _, val := range e.Values {
		ctx.collectCTEReferencesInExpr(val, deps)
	}
}

// handleBinaryExpr handles binary expressions.
func (ctx *CTEContext) handleBinaryExpr(e *parser.BinaryExpr, deps map[string]bool) {
	ctx.collectCTEReferencesInExpr(e.Left, deps)
	ctx.collectCTEReferencesInExpr(e.Right, deps)
}

// handleCaseExpr handles CASE expressions.
func (ctx *CTEContext) handleCaseExpr(e *parser.CaseExpr, deps map[string]bool) {
	if e.Expr != nil {
		ctx.collectCTEReferencesInExpr(e.Expr, deps)
	}
	for _, when := range e.WhenClauses {
		ctx.collectCTEReferencesInExpr(when.Condition, deps)
		ctx.collectCTEReferencesInExpr(when.Result, deps)
	}
	if e.ElseClause != nil {
		ctx.collectCTEReferencesInExpr(e.ElseClause, deps)
	}
}

// handleBetweenExpr handles BETWEEN expressions.
func (ctx *CTEContext) handleBetweenExpr(e *parser.BetweenExpr, deps map[string]bool) {
	ctx.collectCTEReferencesInExpr(e.Expr, deps)
	ctx.collectCTEReferencesInExpr(e.Lower, deps)
	ctx.collectCTEReferencesInExpr(e.Upper, deps)
}

// handleFunctionExpr handles function call expressions.
func (ctx *CTEContext) handleFunctionExpr(e *parser.FunctionExpr, deps map[string]bool) {
	for _, arg := range e.Args {
		ctx.collectCTEReferencesInExpr(arg, deps)
	}
	if e.Filter != nil {
		ctx.collectCTEReferencesInExpr(e.Filter, deps)
	}
}

// buildDependencyOrder creates a topological sort of CTEs based on dependencies.
func (ctx *CTEContext) buildDependencyOrder() error {
	// Calculate dependency levels
	for name := range ctx.CTEs {
		if err := ctx.calculateLevel(name, make(map[string]bool)); err != nil {
			return err
		}
	}

	// Build ordered list
	type levelCTE struct {
		name  string
		level int
	}
	leveledCTEs := make([]levelCTE, 0, len(ctx.CTEs))
	for name, def := range ctx.CTEs {
		leveledCTEs = append(leveledCTEs, levelCTE{name, def.Level})
	}

	// Sort by level
	for i := 0; i < len(leveledCTEs); i++ {
		for j := i + 1; j < len(leveledCTEs); j++ {
			if leveledCTEs[j].level < leveledCTEs[i].level {
				leveledCTEs[i], leveledCTEs[j] = leveledCTEs[j], leveledCTEs[i]
			}
		}
	}

	// Build order
	for _, lc := range leveledCTEs {
		ctx.CTEOrder = append(ctx.CTEOrder, lc.name)
	}

	return nil
}

// calculateLevel calculates the dependency level for a CTE.
func (ctx *CTEContext) calculateLevel(name string, visiting map[string]bool) error {
	def, exists := ctx.CTEs[name]
	if !exists {
		return fmt.Errorf("undefined CTE: %s", name)
	}

	if def.Level > 0 {
		return nil // Already calculated
	}

	if err := ctx.checkLevelCircularity(name, def, visiting); err != nil {
		return err
	}

	visiting[name] = true
	maxLevel, err := ctx.calculateMaxDependencyLevel(name, def, visiting)
	if err != nil {
		return err
	}

	def.Level = maxLevel + 1
	delete(visiting, name)

	return nil
}

func (ctx *CTEContext) checkLevelCircularity(name string, def *CTEDefinition, visiting map[string]bool) error {
	if !visiting[name] {
		return nil
	}

	if def.IsRecursive {
		def.Level = 1
		return nil
	}
	return fmt.Errorf("circular dependency detected in CTE: %s", name)
}

func (ctx *CTEContext) calculateMaxDependencyLevel(name string, def *CTEDefinition, visiting map[string]bool) (int, error) {
	maxLevel := 0

	for _, dep := range def.DependsOn {
		if dep == name {
			if !def.IsRecursive {
				return 0, fmt.Errorf("non-recursive CTE cannot reference itself: %s", name)
			}
			continue
		}

		if err := ctx.calculateLevel(dep, visiting); err != nil {
			return 0, err
		}

		depDef := ctx.CTEs[dep]
		if depDef.Level > maxLevel {
			maxLevel = depDef.Level
		}
	}

	return maxLevel, nil
}

// ExpandCTE expands a CTE reference into a TableInfo.
func (ctx *CTEContext) ExpandCTE(name string, cursor int) (*TableInfo, error) {
	def, exists := ctx.CTEs[name]
	if !exists {
		return nil, fmt.Errorf("undefined CTE: %s", name)
	}

	// Check if already materialized
	if mat, exists := ctx.MaterializedCTEs[name]; exists {
		return &TableInfo{
			Name:      mat.TempTable,
			Alias:     name,
			Cursor:    cursor,
			RowCount:  mat.RowCount,
			RowLogEst: NewLogEst(mat.RowCount),
			Columns:   mat.Columns,
			Indexes:   make([]*IndexInfo, 0),
		}, nil
	}

	// Create a new TableInfo for each expansion with the given cursor
	// Don't reuse def.TableInfo as multiple expansions need independent instances
	return ctx.createTableInfoForCTE(def, cursor), nil
}

// createTableInfoForCTE creates a TableInfo from a CTE definition.
func (ctx *CTEContext) createTableInfoForCTE(def *CTEDefinition, cursor int) *TableInfo {
	// Infer columns from the SELECT statement
	columns := ctx.inferColumns(def)

	return &TableInfo{
		Name:      def.Name,
		Alias:     def.Name,
		Cursor:    cursor,
		RowCount:  def.EstimatedRows.ToInt(),
		RowLogEst: def.EstimatedRows,
		Columns:   columns,
		Indexes:   make([]*IndexInfo, 0),
	}
}

// inferColumns infers column definitions from a CTE's SELECT statement.
func (ctx *CTEContext) inferColumns(def *CTEDefinition) []ColumnInfo {
	columns := make([]ColumnInfo, 0)

	// If explicit column list provided, use it
	if len(def.Columns) > 0 {
		for i, colName := range def.Columns {
			columns = append(columns, ColumnInfo{
				Name:         colName,
				Index:        i,
				Type:         "ANY", // Type inference would be more complex
				NotNull:      false,
				DefaultValue: nil,
			})
		}
		return columns
	}

	// Otherwise infer from SELECT columns
	if def.Select != nil && len(def.Select.Columns) > 0 {
		for i, col := range def.Select.Columns {
			colName := ctx.inferColumnName(col, i)
			columns = append(columns, ColumnInfo{
				Name:         colName,
				Index:        i,
				Type:         "ANY",
				NotNull:      false,
				DefaultValue: nil,
			})
		}
	}

	return columns
}

// inferColumnName infers a column name from a result column.
func (ctx *CTEContext) inferColumnName(col parser.ResultColumn, index int) string {
	if col.Alias != "" {
		return col.Alias
	}

	if col.Star {
		return "*"
	}

	// Try to extract name from expression
	if identExpr, ok := col.Expr.(*parser.IdentExpr); ok {
		return identExpr.Name
	}

	// Default to column index
	return fmt.Sprintf("column_%d", index)
}

// MaterializeCTE materializes a CTE into a temporary table.
func (ctx *CTEContext) MaterializeCTE(name string) (*MaterializedCTE, error) {
	def, exists := ctx.CTEs[name]
	if !exists {
		return nil, fmt.Errorf("undefined CTE: %s", name)
	}

	if mat := ctx.getExistingMaterialization(name); mat != nil {
		return mat, nil
	}

	if err := ctx.materializeDependencies(name, def.DependsOn); err != nil {
		return nil, err
	}

	mat := ctx.createMaterializedCTE(name, def)

	if def.IsRecursive {
		if err := ctx.materializeRecursiveCTE(def, mat); err != nil {
			return nil, err
		}
	}

	ctx.MaterializedCTEs[name] = mat
	return mat, nil
}

// getExistingMaterialization checks if CTE is already materialized.
func (ctx *CTEContext) getExistingMaterialization(name string) *MaterializedCTE {
	if mat, exists := ctx.MaterializedCTEs[name]; exists {
		return mat
	}
	return nil
}

// materializeDependencies materializes all dependencies for a CTE.
func (ctx *CTEContext) materializeDependencies(name string, deps []string) error {
	for _, dep := range deps {
		if dep == name {
			continue // Skip self-reference in recursive CTEs
		}
		if _, err := ctx.MaterializeCTE(dep); err != nil {
			return err
		}
	}
	return nil
}

// createMaterializedCTE creates a new MaterializedCTE instance.
func (ctx *CTEContext) createMaterializedCTE(name string, def *CTEDefinition) *MaterializedCTE {
	return &MaterializedCTE{
		Name:        name,
		TempTable:   fmt.Sprintf("_cte_%s", name),
		Columns:     ctx.inferColumns(def),
		RowCount:    def.EstimatedRows.ToInt(),
		IsRecursive: def.IsRecursive,
		Iterations:  0,
	}
}

// materializeRecursiveCTE handles recursive CTE materialization.
func (ctx *CTEContext) materializeRecursiveCTE(def *CTEDefinition, mat *MaterializedCTE) error {
	// Recursive CTEs are evaluated iteratively:
	// 1. Execute the anchor member (initial query)
	// 2. Execute the recursive member repeatedly until no new rows
	// 3. UNION the results

	// For now, we estimate iterations based on a simple heuristic
	mat.Iterations = 5 // Default estimate
	mat.RowCount = def.EstimatedRows.ToInt()

	return nil
}

// GetCTE returns a CTE definition by name.
func (ctx *CTEContext) GetCTE(name string) (*CTEDefinition, bool) {
	def, exists := ctx.CTEs[name]
	return def, exists
}

// HasCTE checks if a CTE exists.
func (ctx *CTEContext) HasCTE(name string) bool {
	_, exists := ctx.CTEs[name]
	return exists
}

// RewriteQueryWithCTEs rewrites a query to replace CTE references.
// This integrates CTEs into the main query planning process.
func (ctx *CTEContext) RewriteQueryWithCTEs(tables []*TableInfo) ([]*TableInfo, error) {
	if ctx == nil {
		return tables, nil
	}

	result := make([]*TableInfo, 0, len(tables))
	cursor := 0

	for _, table := range tables {
		// Check if this is a CTE reference
		if ctx.HasCTE(table.Name) {
			// Expand CTE to TableInfo
			cteTable, err := ctx.ExpandCTE(table.Name, cursor)
			if err != nil {
				return nil, err
			}
			result = append(result, cteTable)
			cursor++
		} else {
			result = append(result, table)
			cursor++
		}
	}

	return result, nil
}

// EstimateRecursiveCTERows estimates rows for a recursive CTE.
// This uses heuristics similar to SQLite's approach.
func (ctx *CTEContext) EstimateRecursiveCTERows(def *CTEDefinition) LogEst {
	// For recursive CTEs, estimate based on:
	// 1. Anchor member row count
	// 2. Growth factor per iteration
	// 3. Maximum iteration limit

	anchorRows := NewLogEst(10)  // Default anchor estimate
	growthFactor := NewLogEst(2) // Assume 2x growth per iteration
	maxIterations := 10          // Safety limit

	// Estimate total rows: anchor * (1 + growth + growth^2 + ... + growth^n)
	// Simplified: anchor * growth^(iterations/2)
	totalRows := anchorRows
	for i := 0; i < maxIterations/2; i++ {
		totalRows = totalRows.Add(growthFactor)
	}

	return totalRows
}

// ValidateCTEs performs validation checks on CTEs.
func (ctx *CTEContext) ValidateCTEs() error {
	if ctx == nil {
		return nil
	}

	// Check for circular dependencies
	for name := range ctx.CTEs {
		if err := ctx.checkCircularDependency(name, make(map[string]bool)); err != nil {
			return err
		}
	}

	// Validate recursive CTEs
	// If the WITH clause uses RECURSIVE, all CTEs must have proper structure
	if ctx.IsRecursive {
		for name, def := range ctx.CTEs {
			if err := ctx.validateRecursiveCTE(name, def); err != nil {
				return err
			}
		}
	}

	return nil
}

// checkCircularDependency checks for circular dependencies.
func (ctx *CTEContext) checkCircularDependency(name string, visiting map[string]bool) error {
	if err := ctx.checkVisitingCycle(name, visiting); err != nil {
		return err
	}

	def, exists := ctx.CTEs[name]
	if !exists {
		return nil
	}

	visiting[name] = true
	defer delete(visiting, name)

	return ctx.checkDependencies(name, def, visiting)
}

// checkVisitingCycle checks if we're revisiting a CTE that's already being processed
func (ctx *CTEContext) checkVisitingCycle(name string, visiting map[string]bool) error {
	if !visiting[name] {
		return nil
	}

	def := ctx.CTEs[name]
	if def.IsRecursive {
		return nil
	}
	return fmt.Errorf("circular dependency in non-recursive CTE: %s", name)
}

// checkDependencies validates all dependencies of a CTE
func (ctx *CTEContext) checkDependencies(name string, def *CTEDefinition, visiting map[string]bool) error {
	for _, dep := range def.DependsOn {
		if err := ctx.validateDependency(name, dep, def.IsRecursive, visiting); err != nil {
			return err
		}
	}
	return nil
}

// validateDependency checks a single dependency for validity
func (ctx *CTEContext) validateDependency(name, dep string, isRecursive bool, visiting map[string]bool) error {
	if dep == name {
		return ctx.validateSelfReference(name, isRecursive)
	}
	return ctx.checkCircularDependency(dep, visiting)
}

// validateSelfReference checks if a CTE can reference itself
func (ctx *CTEContext) validateSelfReference(name string, isRecursive bool) error {
	if isRecursive {
		return nil
	}
	return fmt.Errorf("non-recursive CTE cannot reference itself: %s", name)
}

// validateRecursiveCTE validates a recursive CTE structure.
func (ctx *CTEContext) validateRecursiveCTE(name string, def *CTEDefinition) error {
	// Recursive CTEs must have:
	// 1. A UNION or UNION ALL
	// 2. An anchor member (doesn't reference the CTE)
	// 3. A recursive member (references the CTE)

	if def.Select == nil {
		return fmt.Errorf("recursive CTE %s has no SELECT", name)
	}

	// Check for UNION - need to check if the SELECT has a compound structure
	if !ctx.hasUnionStructure(def.Select) {
		return fmt.Errorf("recursive CTE %s must use UNION or UNION ALL", name)
	}

	return nil
}

// hasUnionStructure checks if a SELECT statement has UNION or UNION ALL structure.
func (ctx *CTEContext) hasUnionStructure(sel *parser.SelectStmt) bool {
	if sel == nil {
		return false
	}

	// Check if this SELECT has a compound structure
	if sel.Compound != nil {
		compound := sel.Compound
		if compound.Op == parser.CompoundUnion || compound.Op == parser.CompoundUnionAll {
			return true
		}
	}

	return false
}
