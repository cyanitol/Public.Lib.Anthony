package planner

import (
	"fmt"
	"sort"
)

// Planner is the main query planner that generates execution plans.
type Planner struct {
	CostModel         *CostModel
	SubqueryOptimizer *SubqueryOptimizer
	Statistics        *Statistics
	CTEContext        *CTEContext // Common Table Expression context
}

// NewPlanner creates a new query planner.
func NewPlanner() *Planner {
	costModel := NewCostModel()
	return &Planner{
		CostModel:         costModel,
		SubqueryOptimizer: NewSubqueryOptimizer(costModel),
		Statistics:        NewStatistics(),
	}
}

// NewPlannerWithStatistics creates a planner with pre-loaded statistics.
func NewPlannerWithStatistics(stats *Statistics) *Planner {
	costModel := NewCostModel()
	return &Planner{
		CostModel:         costModel,
		SubqueryOptimizer: NewSubqueryOptimizer(costModel),
		Statistics:        stats,
	}
}

// SetStatistics updates the planner's statistics.
func (p *Planner) SetStatistics(stats *Statistics) {
	p.Statistics = stats
}

// GetStatistics returns the planner's current statistics.
func (p *Planner) GetStatistics() *Statistics {
	return p.Statistics
}

// SetCTEContext sets the CTE context for the planner.
func (p *Planner) SetCTEContext(ctx *CTEContext) {
	p.CTEContext = ctx
}

// GetCTEContext returns the planner's CTE context.
func (p *Planner) GetCTEContext() *CTEContext {
	return p.CTEContext
}

// PlanQuery generates an execution plan for a query.
func (p *Planner) PlanQuery(tables []*TableInfo, whereClause *WhereClause) (*WhereInfo, error) {
	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables in query")
	}

	// Phase 0: Expand CTEs and apply statistics
	expandedTables, err := p.prepareTablesForPlanning(tables)
	if err != nil {
		return nil, err
	}

	info := &WhereInfo{
		Clause:   whereClause,
		Tables:   expandedTables,
		AllLoops: make([]*WhereLoop, 0),
	}

	// Phase 1: Optimize WHERE clause if present
	info, err = p.optimizeWhereClause(info, whereClause)
	if err != nil {
		return nil, err
	}

	// Phase 2: Generate all possible WhereLoop objects for each table
	for cursor, table := range expandedTables {
		loops := p.generateLoops(table, cursor, whereClause)
		info.AllLoops = append(info.AllLoops, loops...)
	}

	// Phase 3: Find the optimal query plan
	bestPath, err := p.findBestPath(info)
	if err != nil {
		return nil, err
	}

	info.BestPath = bestPath
	info.NOut = bestPath.NRow

	return info, nil
}

func (p *Planner) prepareTablesForPlanning(tables []*TableInfo) ([]*TableInfo, error) {
	expandedTables := tables
	if p.CTEContext != nil {
		var err error
		expandedTables, err = p.CTEContext.RewriteQueryWithCTEs(tables)
		if err != nil {
			return nil, fmt.Errorf("CTE expansion failed: %w", err)
		}
	}

	if p.Statistics != nil {
		for _, table := range expandedTables {
			ApplyStatisticsToTable(table, p.Statistics)
		}
	}

	return expandedTables, nil
}

func (p *Planner) optimizeWhereClause(info *WhereInfo, whereClause *WhereClause) (*WhereInfo, error) {
	if whereClause == nil {
		return info, nil
	}

	optimizedInfo, err := p.optimizeWhereSubqueries(info)
	if err != nil {
		return nil, fmt.Errorf("subquery optimization failed: %w", err)
	}
	return optimizedInfo, nil
}

// generateLoops generates all WhereLoop options for a single table.
func (p *Planner) generateLoops(table *TableInfo, cursor int, whereClause *WhereClause) []*WhereLoop {
	// Filter terms that apply to this table
	var terms []*WhereTerm
	if whereClause != nil {
		terms = make([]*WhereTerm, 0)
		for _, term := range whereClause.Terms {
			if term.LeftCursor == cursor {
				terms = append(terms, term)
			}
		}
	}

	// Build all possible access paths
	builder := NewWhereLoopBuilder(table, cursor, terms, p.CostModel)
	loops := builder.Build()

	// Also try skip-scan optimization for each index
	for _, index := range table.Indexes {
		if skipLoop := builder.OptimizeForSkipScan(index); skipLoop != nil {
			loops = append(loops, skipLoop)
		}
	}

	return loops
}

// findBestPath finds the optimal sequence of WhereLoops (one per table).
// This implements a dynamic programming algorithm similar to SQLite's solver.
func (p *Planner) findBestPath(info *WhereInfo) (*WherePath, error) {
	nTables := len(info.Tables)

	if nTables == 1 {
		// Single table: just pick the best loop
		return p.findBestSingleTable(info)
	}

	// Multi-table join: use dynamic programming
	return p.findBestMultiTable(info)
}

// findBestSingleTable finds the best plan for a single table.
func (p *Planner) findBestSingleTable(info *WhereInfo) (*WherePath, error) {
	// Find all loops for the single table
	loops := make([]*WhereLoop, 0)
	for _, loop := range info.AllLoops {
		if loop.TabIndex == 0 {
			loops = append(loops, loop)
		}
	}

	if len(loops) == 0 {
		return nil, fmt.Errorf("no access paths found")
	}

	// Select the best loop
	bestLoop := p.CostModel.SelectBestLoop(loops)

	path := &WherePath{
		MaskLoop: bestLoop.MaskSelf,
		NRow:     bestLoop.NOut,
		Cost:     p.CostModel.CalculateLoopCost(bestLoop),
		Loops:    []*WhereLoop{bestLoop},
	}

	return path, nil
}

// findBestMultiTable finds the best plan for multiple tables using dynamic programming.
func (p *Planner) findBestMultiTable(info *WhereInfo) (*WherePath, error) {
	nTables := len(info.Tables)
	const N = 5

	currentPaths := []*WherePath{{
		MaskLoop: 0,
		NRow:     0,
		Cost:     0,
		Loops:    make([]*WhereLoop, 0),
	}}

	for level := 0; level < nTables; level++ {
		nextPaths := p.extendAllPaths(info, currentPaths, nTables)
		if len(nextPaths) == 0 {
			return nil, fmt.Errorf("no valid join order found at level %d", level)
		}
		currentPaths = p.selectBestPaths(nextPaths, N)
	}

	if len(currentPaths) == 0 {
		return nil, fmt.Errorf("no complete path found")
	}
	return currentPaths[0], nil
}

// extendAllPaths extends all current paths with all valid next loops.
func (p *Planner) extendAllPaths(info *WhereInfo, currentPaths []*WherePath, nTables int) []*WherePath {
	nextPaths := make([]*WherePath, 0)
	for _, path := range currentPaths {
		nextPaths = append(nextPaths, p.extendPathWithTables(info, path, nTables)...)
	}
	return nextPaths
}

// extendPathWithTables extends a single path with all valid table loops.
func (p *Planner) extendPathWithTables(info *WhereInfo, path *WherePath, nTables int) []*WherePath {
	result := make([]*WherePath, 0)
	for cursor := 0; cursor < nTables; cursor++ {
		mask := Bitmask(1 << uint(cursor))
		if path.MaskLoop.Overlaps(mask) {
			continue
		}
		result = append(result, p.extendPathWithLoops(info, path, cursor)...)
	}
	return result
}

// extendPathWithLoops extends a path with all valid loops for a cursor.
func (p *Planner) extendPathWithLoops(info *WhereInfo, path *WherePath, cursor int) []*WherePath {
	result := make([]*WherePath, 0)
	for _, loop := range p.findLoopsForTable(info, cursor) {
		if path.MaskLoop.HasAll(loop.Prereq) {
			result = append(result, p.extendPath(path, loop))
		}
	}
	return result
}

// findLoopsForTable finds all loops for a specific table.
func (p *Planner) findLoopsForTable(info *WhereInfo, cursor int) []*WhereLoop {
	loops := make([]*WhereLoop, 0)
	for _, loop := range info.AllLoops {
		if loop.TabIndex == cursor {
			loops = append(loops, loop)
		}
	}
	return loops
}

// extendPath extends a partial path with a new loop.
func (p *Planner) extendPath(path *WherePath, loop *WhereLoop) *WherePath {
	newPath := &WherePath{
		MaskLoop: path.MaskLoop | loop.MaskSelf,
		Loops:    make([]*WhereLoop, len(path.Loops)+1),
	}

	copy(newPath.Loops, path.Loops)
	newPath.Loops[len(path.Loops)] = loop

	// Calculate combined cost and row count
	newPath.Cost, newPath.NRow = p.CostModel.CombineLoopCosts(newPath.Loops)

	return newPath
}

// selectBestPaths selects the top N paths by cost.
func (p *Planner) selectBestPaths(paths []*WherePath, n int) []*WherePath {
	// Sort by cost (ascending)
	sort.Slice(paths, func(i, j int) bool {
		// Primary: cost
		if paths[i].Cost != paths[j].Cost {
			return paths[i].Cost < paths[j].Cost
		}
		// Tie-breaker: output rows
		return paths[i].NRow < paths[j].NRow
	})

	// Keep top N
	if len(paths) > n {
		paths = paths[:n]
	}

	return paths
}

// OptimizeWhereClause analyzes and optimizes WHERE clause terms.
func (p *Planner) OptimizeWhereClause(expr Expr, tables []*TableInfo) (*WhereClause, error) {
	clause := &WhereClause{
		Terms: make([]*WhereTerm, 0),
	}

	// Split AND terms
	andTerms := p.splitAnd(expr)

	// Convert each term to WhereTerm
	for _, term := range andTerms {
		whereTerm, err := p.analyzeExpr(term, tables)
		if err != nil {
			return nil, err
		}
		if whereTerm != nil {
			clause.Terms = append(clause.Terms, whereTerm)
			if whereTerm.Operator == WO_OR {
				clause.HasOr = true
			}
		}
	}

	// Apply transitive closure (if a=b and b=c, add a=c)
	p.applyTransitiveClosure(clause)

	return clause, nil
}

// splitAnd recursively splits AND expressions.
func (p *Planner) splitAnd(expr Expr) []Expr {
	if andExpr, ok := expr.(*AndExpr); ok {
		result := make([]Expr, 0)
		for _, term := range andExpr.Terms {
			result = append(result, p.splitAnd(term)...)
		}
		return result
	}
	return []Expr{expr}
}

// analyzeExpr analyzes a single expression and creates a WhereTerm.
func (p *Planner) analyzeExpr(expr Expr, tables []*TableInfo) (*WhereTerm, error) {
	binExpr, ok := expr.(*BinaryExpr)
	if !ok {
		// Not a binary expression, might be OR or other complex expr
		if orExpr, ok := expr.(*OrExpr); ok {
			return p.analyzeOrExpr(orExpr, tables)
		}
		return nil, nil
	}

	// Extract column reference
	colExpr, ok := binExpr.Left.(*ColumnExpr)
	if !ok {
		return nil, nil
	}

	// Determine operator
	op := p.parseOperator(binExpr.Op)
	if op == 0 {
		return nil, nil // Unknown operator
	}

	// Find column index
	colIdx := -1
	for i, col := range tables[colExpr.Cursor].Columns {
		if col.Name == colExpr.Column {
			colIdx = i
			break
		}
	}

	// Extract right-side value
	var rightValue interface{}
	if valExpr, ok := binExpr.Right.(*ValueExpr); ok {
		rightValue = valExpr.Value
	}

	term := &WhereTerm{
		Expr:        expr,
		Operator:    op,
		LeftCursor:  colExpr.Cursor,
		LeftColumn:  colIdx,
		RightValue:  rightValue,
		PrereqRight: binExpr.Right.UsedTables(),
		PrereqAll:   expr.UsedTables(),
		TruthProb:   0, // Will be estimated later
		Flags:       0,
		Parent:      -1,
	}

	return term, nil
}

// analyzeOrExpr handles OR expressions specially.
func (p *Planner) analyzeOrExpr(expr *OrExpr, tables []*TableInfo) (*WhereTerm, error) {
	term := &WhereTerm{
		Expr:       expr,
		Operator:   WO_OR,
		LeftCursor: -1,
		LeftColumn: -1,
		PrereqAll:  expr.UsedTables(),
		TruthProb:  0,
		Flags:      0,
		Parent:     -1,
	}

	return term, nil
}

// parseOperatorMap maps string operators to WhereOperator values.
var parseOperatorMap = map[string]WhereOperator{
	"=":       WO_EQ,
	"<":       WO_LT,
	"<=":      WO_LE,
	">":       WO_GT,
	">=":      WO_GE,
	"IN":      WO_IN,
	"IS":      WO_IS,
	"IS NULL": WO_ISNULL,
}

// parseOperator converts string operator to WhereOperator.
func (p *Planner) parseOperator(op string) WhereOperator {
	if wo, ok := parseOperatorMap[op]; ok {
		return wo
	}
	return 0
}

// applyTransitiveClosure adds implied constraints from transitive relationships.
// For example, if we have a=b and b=5, we can infer a=5.
func (p *Planner) applyTransitiveClosure(clause *WhereClause) {
	equiv := p.buildEquivalenceClasses(clause)
	newTerms := p.propagateConstants(clause, equiv)
	clause.Terms = append(clause.Terms, newTerms...)
}

// buildEquivalenceClasses builds a map of equivalent columns.
func (p *Planner) buildEquivalenceClasses(clause *WhereClause) map[string][]string {
	equiv := make(map[string][]string)
	for _, term := range clause.Terms {
		if term.Operator != WO_EQ {
			continue
		}
		colExpr, ok := term.Expr.(*BinaryExpr)
		if !ok {
			continue
		}
		rightCol, ok := colExpr.Right.(*ColumnExpr)
		if !ok {
			continue
		}
		leftKey := fmt.Sprintf("%d.%d", term.LeftCursor, term.LeftColumn)
		rightKey := fmt.Sprintf("%d.%d", rightCol.Cursor, rightCol.UsedTables())
		equiv[leftKey] = append(equiv[leftKey], rightKey)
		equiv[rightKey] = append(equiv[rightKey], leftKey)
	}
	return equiv
}

// propagateConstants creates new terms for equivalent columns with constant values.
func (p *Planner) propagateConstants(clause *WhereClause, equiv map[string][]string) []*WhereTerm {
	newTerms := make([]*WhereTerm, 0)
	for _, term := range clause.Terms {
		if term.Operator != WO_EQ || term.RightValue == nil {
			continue
		}
		if _, ok := term.Expr.(*BinaryExpr); !ok {
			continue
		}
		leftKey := fmt.Sprintf("%d.%d", term.LeftCursor, term.LeftColumn)
		for _, equivKey := range equiv[leftKey] {
			newTerms = append(newTerms, p.createEquivTerm(term, equivKey))
		}
	}
	return newTerms
}

// createEquivTerm creates a new equivalent term for a column.
func (p *Planner) createEquivTerm(term *WhereTerm, equivKey string) *WhereTerm {
	var cursor, column int
	fmt.Sscanf(equivKey, "%d.%d", &cursor, &column)
	return &WhereTerm{
		Expr:        term.Expr,
		Operator:    WO_EQ,
		LeftCursor:  cursor,
		LeftColumn:  column,
		RightValue:  term.RightValue,
		PrereqRight: 0,
		PrereqAll:   term.PrereqAll,
		TruthProb:   term.TruthProb,
		Flags:       TERM_VIRTUAL,
		Parent:      -1,
	}
}

// ExplainPlan returns a human-readable explanation of the query plan.
// This is enhanced in Phase 9.3 to include detailed cost estimates.
func (p *Planner) ExplainPlan(info *WhereInfo) string {
	if info.BestPath == nil {
		return "No plan available"
	}

	result := "QUERY PLAN:\n"
	result += fmt.Sprintf("├─ Estimated output rows: %d\n", info.BestPath.NRow.ToInt())
	result += fmt.Sprintf("├─ Estimated cost: %.2f\n", float64(info.BestPath.Cost))

	// Add cost breakdown
	totalCost := float64(info.BestPath.Cost)
	if totalCost > 0 {
		result += fmt.Sprintf("├─ Cost breakdown:\n")
		cpuCost := totalCost * 0.6  // Estimated 60% CPU
		ioCost := totalCost * 0.4   // Estimated 40% I/O
		result += fmt.Sprintf("│  ├─ CPU cost: %.2f (%.1f%%)\n", cpuCost, 60.0)
		result += fmt.Sprintf("│  └─ I/O cost: %.2f (%.1f%%)\n", ioCost, 40.0)
	}
	result += "└─ Execution steps:\n"

	for i, loop := range info.BestPath.Loops {
		result += p.explainLoopDetailed(info, loop, i, len(info.BestPath.Loops))
	}

	return result
}

// explainLoop returns a human-readable explanation for a single loop.
func (p *Planner) explainLoop(info *WhereInfo, loop *WhereLoop, i int) string {
	table := info.Tables[loop.TabIndex]
	indent := makeIndent(i)

	var result string
	if loop.Index != nil {
		result = p.explainIndexLoop(table, loop, indent, i)
	} else {
		result = fmt.Sprintf("%s%d. SCAN %s\n", indent, i+1, table.Name)
	}
	result += fmt.Sprintf("%s   Cost: %d, Rows: %d\n", indent, loop.Run.ToInt(), loop.NOut.ToInt())
	return result
}

// explainLoopDetailed returns a detailed explanation for a single loop (Phase 9.3).
func (p *Planner) explainLoopDetailed(info *WhereInfo, loop *WhereLoop, i int, totalLoops int) string {
	table := info.Tables[loop.TabIndex]
	isLast := (i == totalLoops-1)
	prefix := "   "
	if isLast {
		prefix += "└─ "
	} else {
		prefix += "├─ "
	}

	var result string
	stepNum := i + 1

	if loop.Index != nil {
		// Index-based access
		result = fmt.Sprintf("%s%d. INDEX SEARCH on %s\n", prefix, stepNum, table.Name)

		subPrefix := "   "
		if !isLast {
			subPrefix += "│  "
		} else {
			subPrefix += "   "
		}

		result += fmt.Sprintf("%s├─ Index: %s\n", subPrefix, loop.Index.Name)
		result += fmt.Sprintf("%s├─ Columns: %v\n", subPrefix, loop.Index.Columns)

		// Add constraint information
		constraints := p.buildConstraintStrings(table, loop)
		if len(constraints) > 0 {
			result += fmt.Sprintf("%s├─ Constraints: %s\n", subPrefix, joinStrings(constraints, ", "))
		}

		// Add selectivity estimate
		selectivity := p.estimateSelectivity(loop)
		result += fmt.Sprintf("%s├─ Selectivity: %.4f (%.2f%%)\n", subPrefix, selectivity, selectivity*100)

		// Cost details
		result += fmt.Sprintf("%s├─ Estimated cost: %.2f\n", subPrefix, float64(loop.Run.ToInt()))
		result += fmt.Sprintf("%s├─ Estimated rows out: %d\n", subPrefix, loop.NOut.ToInt())

		// Index characteristics
		if loop.Index.Unique {
			result += fmt.Sprintf("%s└─ Index type: UNIQUE\n", subPrefix)
		} else {
			result += fmt.Sprintf("%s└─ Index type: NON-UNIQUE\n", subPrefix)
		}
	} else {
		// Full table scan
		result = fmt.Sprintf("%s%d. FULL TABLE SCAN on %s\n", prefix, stepNum, table.Name)

		subPrefix := "   "
		if !isLast {
			subPrefix += "│  "
		} else {
			subPrefix += "   "
		}

		result += fmt.Sprintf("%s├─ Table rows: %d\n", subPrefix, table.RowCount)
		result += fmt.Sprintf("%s├─ Estimated cost: %.2f\n", subPrefix, float64(loop.Run.ToInt()))
		result += fmt.Sprintf("%s├─ Estimated rows out: %d\n", subPrefix, loop.NOut.ToInt())
		result += fmt.Sprintf("%s└─ Access method: Sequential\n", subPrefix)
	}

	return result
}

// estimateSelectivity estimates the selectivity of a where loop (0.0 to 1.0).
func (p *Planner) estimateSelectivity(loop *WhereLoop) float64 {
	if len(loop.Terms) == 0 {
		return 1.0 // No constraints, all rows selected
	}

	// Use product of individual term selectivities
	selectivity := 1.0
	for _, term := range loop.Terms {
		termSelectivity := p.estimateTermSelectivity(term)
		selectivity *= termSelectivity
	}

	return selectivity
}

// estimateTermSelectivity estimates the selectivity of a single where term.
func (p *Planner) estimateTermSelectivity(term *WhereTerm) float64 {
	// Default selectivity estimates based on operator
	switch term.Operator {
	case WO_EQ:
		// Equality: typically very selective
		return 0.01 // 1% selectivity
	case WO_LT, WO_LE, WO_GT, WO_GE:
		// Range: moderately selective
		return 0.33 // 33% selectivity
	case WO_IN:
		// IN clause: depends on list size, estimate medium
		return 0.10 // 10% selectivity
	case WO_IS, WO_ISNULL:
		// IS NULL: typically rare
		return 0.05 // 5% selectivity
	default:
		// Default selectivity for other operators (including LIKE via flags)
		return 0.50 // 50% default
	}
}

// makeIndent creates an indentation string for the given level.
func makeIndent(level int) string {
	indent := ""
	for j := 0; j < level; j++ {
		indent += "  "
	}
	return indent
}

// explainIndexLoop explains an index-based loop.
func (p *Planner) explainIndexLoop(table *TableInfo, loop *WhereLoop, indent string, i int) string {
	result := fmt.Sprintf("%s%d. SEARCH %s USING INDEX %s", indent, i+1, table.Name, loop.Index.Name)
	constraints := p.buildConstraintStrings(table, loop)
	if len(constraints) > 0 {
		result += " (" + joinStrings(constraints, " AND ") + ")"
	}
	return result + "\n"
}

// buildConstraintStrings builds constraint descriptions for explain output.
func (p *Planner) buildConstraintStrings(table *TableInfo, loop *WhereLoop) []string {
	constraints := make([]string, 0)
	for _, term := range loop.Terms {
		if term.LeftColumn >= 0 && term.LeftColumn < len(table.Columns) {
			col := table.Columns[term.LeftColumn].Name
			op := operatorString(term.Operator)
			constraints = append(constraints, fmt.Sprintf("%s%s?", col, op))
		}
	}
	return constraints
}

// joinStrings joins strings with a separator (helper function).
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}

// ValidatePlan performs sanity checks on a query plan.
func (p *Planner) ValidatePlan(info *WhereInfo) error {
	if info.BestPath == nil {
		return fmt.Errorf("no plan generated")
	}

	// Check that we have a loop for each table
	if len(info.BestPath.Loops) != len(info.Tables) {
		return fmt.Errorf("plan has %d loops but %d tables",
			len(info.BestPath.Loops), len(info.Tables))
	}

	// Check that each table appears exactly once
	seen := make(map[int]bool)
	for _, loop := range info.BestPath.Loops {
		if seen[loop.TabIndex] {
			return fmt.Errorf("table %d appears multiple times in plan", loop.TabIndex)
		}
		seen[loop.TabIndex] = true
	}

	// Check that prerequisites are satisfied
	available := Bitmask(0)
	for _, loop := range info.BestPath.Loops {
		if !available.HasAll(loop.Prereq) {
			return fmt.Errorf("prerequisites not satisfied for table %d", loop.TabIndex)
		}
		available |= loop.MaskSelf
	}

	return nil
}

// optimizeWhereSubqueries detects and optimizes subqueries in WHERE clause.
func (p *Planner) optimizeWhereSubqueries(info *WhereInfo) (*WhereInfo, error) {
	if info.Clause == nil {
		return info, nil
	}

	// Scan WHERE terms for subqueries
	for _, term := range info.Clause.Terms {
		subqueryInfo := p.detectSubquery(term.Expr)
		if subqueryInfo != nil {
			// Analyze the subquery
			analyzed, err := p.SubqueryOptimizer.AnalyzeSubquery(subqueryInfo.Expr, info.Tables)
			if err != nil {
				continue // Skip optimization on error
			}

			// Apply optimization
			optimized, err := p.SubqueryOptimizer.OptimizeSubquery(analyzed, info)
			if err != nil {
				continue // Skip if optimization fails
			}

			// Update info with optimized plan
			info = optimized
		}
	}

	return info, nil
}

// detectSubquery detects if an expression contains a subquery.
func (p *Planner) detectSubquery(expr Expr) *SubqueryInfo {
	if expr == nil {
		return nil
	}

	// Check for SubqueryExpr
	if subExpr, ok := expr.(*SubqueryExpr); ok {
		return &SubqueryInfo{
			Type:          subExpr.Type,
			Expr:          subExpr.Query,
			EstimatedRows: NewLogEst(100),
		}
	}

	// Check for binary expressions that might contain subqueries
	if binExpr, ok := expr.(*BinaryExpr); ok {
		// Check if this is an IN operator
		if binExpr.Op == "IN" {
			return &SubqueryInfo{
				Type:          SubqueryIn,
				Expr:          binExpr.Right,
				EstimatedRows: NewLogEst(100),
			}
		}

		// Recursively check left and right
		if left := p.detectSubquery(binExpr.Left); left != nil {
			return left
		}
		if right := p.detectSubquery(binExpr.Right); right != nil {
			return right
		}
	}

	return nil
}

// PlanQueryWithSubqueries plans a query that may contain subqueries.
// This is the main entry point for queries with FROM subqueries or complex WHERE clauses.
func (p *Planner) PlanQueryWithSubqueries(tables []*TableInfo, fromSubqueries []Expr, whereClause *WhereClause) (*WhereInfo, error) {
	optimizedTables := make([]*TableInfo, 0, len(tables)+len(fromSubqueries))
	optimizedTables = append(optimizedTables, tables...)

	for _, subquery := range fromSubqueries {
		table, err := p.optimizeFromSubquery(subquery, tables, len(optimizedTables))
		if err != nil {
			return nil, err
		}
		if table != nil {
			optimizedTables = append(optimizedTables, table)
		}
	}

	return p.PlanQuery(optimizedTables, whereClause)
}

// optimizeFromSubquery optimizes a single FROM subquery.
func (p *Planner) optimizeFromSubquery(subquery Expr, tables []*TableInfo, cursor int) (*TableInfo, error) {
	subqueryInfo, err := p.SubqueryOptimizer.AnalyzeSubquery(subquery, tables)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze FROM subquery: %w", err)
	}

	if subqueryInfo.CanFlatten {
		return p.createFlattenedSubqueryTable(subqueryInfo, cursor), nil
	}

	if subqueryInfo.CanMaterialize {
		return p.createMaterializedSubqueryTable(subqueryInfo, cursor)
	}

	return nil, nil
}

// createFlattenedSubqueryTable creates a TableInfo for a flattened subquery.
func (p *Planner) createFlattenedSubqueryTable(info *SubqueryInfo, cursor int) *TableInfo {
	return &TableInfo{
		Name:      info.MaterializedTable,
		Alias:     fmt.Sprintf("subq_%d", cursor),
		Cursor:    cursor,
		RowCount:  info.EstimatedRows.ToInt(),
		RowLogEst: info.EstimatedRows,
		Columns:   make([]ColumnInfo, 0),
		Indexes:   make([]*IndexInfo, 0),
	}
}

// createMaterializedSubqueryTable creates a TableInfo for a materialized subquery.
func (p *Planner) createMaterializedSubqueryTable(info *SubqueryInfo, cursor int) (*TableInfo, error) {
	materialized, err := p.SubqueryOptimizer.MaterializeSubquery(info)
	if err != nil {
		return nil, fmt.Errorf("failed to materialize subquery: %w", err)
	}

	return &TableInfo{
		Name:      materialized.MaterializedTable,
		Alias:     materialized.MaterializedTable,
		Cursor:    cursor,
		RowCount:  materialized.EstimatedRows.ToInt(),
		RowLogEst: materialized.EstimatedRows,
		Columns:   make([]ColumnInfo, 0),
		Indexes:   make([]*IndexInfo, 0),
	}, nil
}
