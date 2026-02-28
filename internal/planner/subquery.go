package planner

import (
	"fmt"
)

// SubqueryType identifies the type of subquery in a SQL statement.
type SubqueryType int

const (
	// SubqueryScalar represents a scalar subquery (returns single value).
	// Example: SELECT (SELECT MAX(price) FROM products) AS max_price
	SubqueryScalar SubqueryType = iota

	// SubqueryExists represents an EXISTS subquery.
	// Example: SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)
	SubqueryExists

	// SubqueryIn represents an IN subquery.
	// Example: SELECT * FROM users WHERE id IN (SELECT user_id FROM premium_users)
	SubqueryIn

	// SubqueryFrom represents a subquery in FROM clause.
	// Example: SELECT * FROM (SELECT * FROM users WHERE active = 1) AS active_users
	SubqueryFrom
)

// String returns the string representation of a SubqueryType.
func (t SubqueryType) String() string {
	switch t {
	case SubqueryScalar:
		return "SCALAR"
	case SubqueryExists:
		return "EXISTS"
	case SubqueryIn:
		return "IN"
	case SubqueryFrom:
		return "FROM"
	default:
		return "UNKNOWN"
	}
}

// SubqueryInfo contains information about a subquery and its optimization potential.
type SubqueryInfo struct {
	// Type is the type of subquery
	Type SubqueryType

	// Expr is the subquery expression (if applicable)
	Expr Expr

	// IsCorrelated indicates if this is a correlated subquery
	// (references columns from outer query)
	IsCorrelated bool

	// OuterRefs is bitmask of outer tables referenced
	OuterRefs Bitmask

	// EstimatedRows is the estimated number of rows returned
	EstimatedRows LogEst

	// ExecutionCount is estimated number of times subquery will execute
	// (1 for uncorrelated, outer table rows for correlated)
	ExecutionCount LogEst

	// CanFlatten indicates if this subquery can be flattened into parent
	CanFlatten bool

	// CanMaterialize indicates if materialization would be beneficial
	CanMaterialize bool

	// MaterializedTable is the temp table name if materialized
	MaterializedTable string
}

// SubqueryOptimizer handles subquery optimization strategies.
type SubqueryOptimizer struct {
	// CostModel for estimating costs
	CostModel *CostModel

	// NextTempTable counter for generating temp table names
	NextTempTable int
}

// NewSubqueryOptimizer creates a new subquery optimizer.
func NewSubqueryOptimizer(costModel *CostModel) *SubqueryOptimizer {
	return &SubqueryOptimizer{
		CostModel:     costModel,
		NextTempTable: 1,
	}
}

// AnalyzeSubquery analyzes a subquery and determines optimization strategies.
func (o *SubqueryOptimizer) AnalyzeSubquery(subquery Expr, outerTables []*TableInfo) (*SubqueryInfo, error) {
	info := &SubqueryInfo{
		Type:          SubqueryScalar, // Default, will be refined
		Expr:          subquery,
		EstimatedRows: NewLogEst(100), // Default estimate
	}

	// Analyze the subquery expression to determine type and characteristics
	info.OuterRefs = o.findOuterReferences(subquery, outerTables)
	info.IsCorrelated = info.OuterRefs != 0

	// Determine if subquery can be flattened
	info.CanFlatten = o.canFlattenSubquery(info)

	// Determine if materialization would be beneficial
	info.CanMaterialize = o.shouldMaterializeSubquery(info)

	return info, nil
}

// FlattenSubquery attempts to merge a simple subquery into the parent query.
// This optimization eliminates the subquery execution overhead by converting
// it into a JOIN or inline expression.
//
// Flattening rules:
// - Subquery must not be correlated (or simple correlation)
// - No aggregates in subquery or compatible aggregates
// - No LIMIT/OFFSET that would change semantics
// - Parent query compatible with additional join
func (o *SubqueryOptimizer) FlattenSubquery(info *SubqueryInfo, parentInfo *WhereInfo) (*WhereInfo, error) {
	if !info.CanFlatten {
		return nil, fmt.Errorf("subquery cannot be flattened")
	}

	// Create a new WhereInfo with the subquery merged
	// In a full implementation, this would:
	// 1. Extract tables from subquery FROM clause
	// 2. Add them to parent's table list
	// 3. Merge WHERE conditions
	// 4. Update column references

	newInfo := &WhereInfo{
		Clause:   parentInfo.Clause,
		Tables:   make([]*TableInfo, len(parentInfo.Tables)),
		AllLoops: make([]*WhereLoop, len(parentInfo.AllLoops)),
		BestPath: parentInfo.BestPath,
		NOut:     parentInfo.NOut,
	}

	copy(newInfo.Tables, parentInfo.Tables)
	copy(newInfo.AllLoops, parentInfo.AllLoops)

	// Mark that flattening was applied
	// In real implementation, would modify the query plan

	return newInfo, nil
}

// DecorrelateSubquery converts a correlated subquery to an uncorrelated one.
// This optimization allows the subquery to be executed once and materialized,
// rather than executing for each outer row.
//
// Decorrelation strategies:
// - Convert correlated EXISTS to semi-join
// - Convert correlated IN to hash join
// - Extract correlation predicates and apply them post-join
func (o *SubqueryOptimizer) DecorrelateSubquery(info *SubqueryInfo) (*SubqueryInfo, error) {
	if !info.IsCorrelated {
		return info, nil // Already uncorrelated
	}

	// Create a decorrelated version
	decorrelated := &SubqueryInfo{
		Type:              info.Type,
		Expr:              info.Expr,
		IsCorrelated:      false, // Now uncorrelated
		OuterRefs:         0,
		EstimatedRows:     info.EstimatedRows,
		ExecutionCount:    NewLogEst(1), // Execute once instead of per outer row
		CanFlatten:        true,         // May now be flattenable
		CanMaterialize:    true,         // Can materialize since uncorrelated
		MaterializedTable: "",
	}

	// In a full implementation, this would transform the subquery:
	// Before: SELECT * FROM orders WHERE order_id IN (SELECT order_id FROM items WHERE items.user_id = orders.user_id)
	// After:  SELECT * FROM orders JOIN (SELECT DISTINCT order_id, user_id FROM items) t ON orders.order_id = t.order_id AND orders.user_id = t.user_id

	return decorrelated, nil
}

// MaterializeSubquery creates a temporary table to hold subquery results.
// This optimization is beneficial when:
// - Subquery is executed multiple times (correlated)
// - Subquery result is small enough to fit in memory
// - Cost of materialization < cost of repeated execution
func (o *SubqueryOptimizer) MaterializeSubquery(info *SubqueryInfo) (*SubqueryInfo, error) {
	if !info.CanMaterialize {
		return nil, fmt.Errorf("subquery cannot be materialized")
	}

	// Generate temp table name
	tempTableName := fmt.Sprintf("_temp_subquery_%d", o.NextTempTable)
	o.NextTempTable++

	materialized := &SubqueryInfo{
		Type:              info.Type,
		Expr:              info.Expr,
		IsCorrelated:      false, // Materialized subqueries become uncorrelated
		OuterRefs:         info.OuterRefs,
		EstimatedRows:     info.EstimatedRows,
		ExecutionCount:    NewLogEst(1), // Execute once to materialize
		CanFlatten:        false,        // Already materialized
		CanMaterialize:    false,        // Already materialized
		MaterializedTable: tempTableName,
	}

	// In a full implementation, this would:
	// 1. Generate code to create temp table
	// 2. Execute subquery and INSERT results into temp table
	// 3. Replace subquery references with temp table scans
	// 4. Generate code to drop temp table at end

	return materialized, nil
}

// ConvertInToJoin converts an IN subquery to a semi-join when beneficial.
// This is typically faster for large result sets.
//
// Before: SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)
// After:  SELECT DISTINCT users.* FROM users INNER JOIN orders ON users.id = orders.user_id
func (o *SubqueryOptimizer) ConvertInToJoin(info *SubqueryInfo, parentInfo *WhereInfo) (*WhereInfo, error) {
	if info.Type != SubqueryIn {
		return nil, fmt.Errorf("not an IN subquery")
	}

	// Estimate cost of IN vs JOIN
	inCost := o.estimateInCost(info, parentInfo)
	joinCost := o.estimateJoinCost(info, parentInfo)

	if joinCost >= inCost {
		return nil, fmt.Errorf("JOIN not beneficial, IN is cheaper")
	}

	// Create modified WhereInfo with semi-join
	newInfo := &WhereInfo{
		Clause:   parentInfo.Clause,
		Tables:   make([]*TableInfo, len(parentInfo.Tables)),
		AllLoops: make([]*WhereLoop, len(parentInfo.AllLoops)),
		BestPath: parentInfo.BestPath,
		NOut:     parentInfo.NOut,
	}

	copy(newInfo.Tables, parentInfo.Tables)
	copy(newInfo.AllLoops, parentInfo.AllLoops)

	// In real implementation, would add semi-join loop and update plan

	return newInfo, nil
}

// ConvertExistsToSemiJoin converts an EXISTS subquery to a semi-join.
// Semi-joins are generally more efficient than executing EXISTS for each row.
//
// Before: SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)
// After:  SELECT DISTINCT orders.* FROM orders INNER JOIN items ON items.order_id = orders.id
func (o *SubqueryOptimizer) ConvertExistsToSemiJoin(info *SubqueryInfo, parentInfo *WhereInfo) (*WhereInfo, error) {
	if info.Type != SubqueryExists {
		return nil, fmt.Errorf("not an EXISTS subquery")
	}

	// Estimate cost of EXISTS vs semi-join
	existsCost := o.estimateExistsCost(info, parentInfo)
	joinCost := o.estimateJoinCost(info, parentInfo)

	if joinCost >= existsCost {
		return nil, fmt.Errorf("semi-join not beneficial, EXISTS is cheaper")
	}

	// Create modified WhereInfo with semi-join
	newInfo := &WhereInfo{
		Clause:   parentInfo.Clause,
		Tables:   make([]*TableInfo, len(parentInfo.Tables)),
		AllLoops: make([]*WhereLoop, len(parentInfo.AllLoops)),
		BestPath: parentInfo.BestPath,
		NOut:     parentInfo.NOut,
	}

	copy(newInfo.Tables, parentInfo.Tables)
	copy(newInfo.AllLoops, parentInfo.AllLoops)

	// In real implementation, would add semi-join loop and update plan

	return newInfo, nil
}

// Helper methods

// findOuterReferences identifies which outer tables are referenced by the subquery.
func (o *SubqueryOptimizer) findOuterReferences(expr Expr, outerTables []*TableInfo) Bitmask {
	var refs Bitmask

	// Walk the expression tree and find column references
	// that belong to outer tables
	if expr != nil {
		// Use the expression's UsedTables() method
		refs = expr.UsedTables()

		// Filter to only include outer table references
		// This is a simplified implementation
		// In a full version, would need to distinguish inner vs outer table refs
	}

	return refs
}

// canFlattenSubquery determines if a subquery can be safely flattened.
func (o *SubqueryOptimizer) canFlattenSubquery(info *SubqueryInfo) bool {
	// Flattening criteria:
	// 1. FROM subqueries are good candidates
	// 2. Simple scalar subqueries without aggregates
	// 3. Not correlated (or simple correlation that can be converted to join)
	// 4. No LIMIT/OFFSET that would change semantics

	if info.Type == SubqueryFrom {
		return true // FROM subqueries are usually flattenable
	}

	if info.Type == SubqueryScalar && !info.IsCorrelated {
		return true // Simple scalar subqueries
	}

	return false
}

// shouldMaterializeSubquery determines if materialization would be beneficial.
func (o *SubqueryOptimizer) shouldMaterializeSubquery(info *SubqueryInfo) bool {
	// Materialization is beneficial when:
	// 1. Subquery is executed multiple times (correlated)
	// 2. Result set is reasonably small
	// 3. Cost of materialization + lookups < cost of repeated execution

	if !info.IsCorrelated {
		// Uncorrelated subqueries execute once anyway
		return false
	}

	// Estimate materialization cost
	materializeCost := info.EstimatedRows // Cost to execute once and store
	repeatCost := info.ExecutionCount.Add(info.EstimatedRows)

	// Materialize if it saves cost
	return materializeCost.ToInt() < repeatCost.ToInt()
}

// estimateInCost estimates the cost of executing an IN subquery.
func (o *SubqueryOptimizer) estimateInCost(info *SubqueryInfo, parentInfo *WhereInfo) LogEst {
	// Cost = outer rows * subquery execution cost
	outerRows := parentInfo.NOut
	subqueryCost := info.EstimatedRows

	return outerRows.Add(subqueryCost)
}

// estimateJoinCost estimates the cost of executing as a semi-join.
func (o *SubqueryOptimizer) estimateJoinCost(info *SubqueryInfo, parentInfo *WhereInfo) LogEst {
	// Cost = outer rows + inner rows + join overhead
	outerRows := parentInfo.NOut
	innerRows := info.EstimatedRows

	// Semi-join typically cheaper than nested loop
	// Cost is roughly: outerRows + innerRows + log(innerRows) * outerRows
	joinCost := outerRows.Add(innerRows)
	return joinCost
}

// estimateExistsCost estimates the cost of executing an EXISTS subquery.
func (o *SubqueryOptimizer) estimateExistsCost(info *SubqueryInfo, parentInfo *WhereInfo) LogEst {
	// EXISTS can short-circuit after finding first match
	// Cost = outer rows * estimated cost to find first match
	outerRows := parentInfo.NOut

	// Assume we find a match after scanning 10% of rows on average
	avgScanCost := info.EstimatedRows.Subtract(NewLogEst(10))

	return outerRows.Add(avgScanCost)
}

// OptimizeSubquery applies the best optimization strategy for a subquery.
func (o *SubqueryOptimizer) OptimizeSubquery(info *SubqueryInfo, parentInfo *WhereInfo) (*WhereInfo, error) {
	// Strategy selection:
	// 1. Try flattening first (eliminates subquery entirely)
	// 2. Try decorrelation if correlated
	// 3. Convert EXISTS to semi-join
	// 4. Convert IN to join
	// 5. Materialize if beneficial

	// Try flattening
	if result, ok := o.tryFlatten(info, parentInfo); ok {
		return result, nil
	}

	// Try decorrelation
	info = o.tryDecorrelate(info)

	// Try type-specific optimizations
	if result, ok := o.tryTypeSpecificOptimization(info, parentInfo); ok {
		return result, nil
	}

	// Try materialization
	if result, ok := o.tryMaterialize(info, parentInfo); ok {
		return result, nil
	}

	// No optimization applied, return original
	return parentInfo, nil
}

// tryFlatten attempts to flatten a subquery and returns the result if successful
func (o *SubqueryOptimizer) tryFlatten(info *SubqueryInfo, parentInfo *WhereInfo) (*WhereInfo, bool) {
	if info.CanFlatten {
		flattened, err := o.FlattenSubquery(info, parentInfo)
		if err == nil {
			return flattened, true
		}
	}
	return nil, false
}

// tryDecorrelate attempts to decorrelate a subquery and returns the updated info
func (o *SubqueryOptimizer) tryDecorrelate(info *SubqueryInfo) *SubqueryInfo {
	if info.IsCorrelated {
		decorrelated, err := o.DecorrelateSubquery(info)
		if err == nil {
			return decorrelated
		}
	}
	return info
}

// tryTypeSpecificOptimization applies type-specific optimizations (EXISTS, IN)
func (o *SubqueryOptimizer) tryTypeSpecificOptimization(info *SubqueryInfo, parentInfo *WhereInfo) (*WhereInfo, bool) {
	if info.Type == SubqueryExists {
		optimized, err := o.ConvertExistsToSemiJoin(info, parentInfo)
		if err == nil {
			return optimized, true
		}
	}

	if info.Type == SubqueryIn {
		optimized, err := o.ConvertInToJoin(info, parentInfo)
		if err == nil {
			return optimized, true
		}
	}

	return nil, false
}

// tryMaterialize attempts to materialize a subquery
func (o *SubqueryOptimizer) tryMaterialize(info *SubqueryInfo, parentInfo *WhereInfo) (*WhereInfo, bool) {
	if info.CanMaterialize {
		_, err := o.MaterializeSubquery(info)
		if err == nil {
			// Update parent info to use materialized table
			// This is a simplified return - real implementation would modify parentInfo
			return parentInfo, true
		}
	}
	return nil, false
}

// SubqueryExpr represents a subquery expression in the planner.
type SubqueryExpr struct {
	// Query is the subquery
	Query Expr

	// Type is the subquery type
	Type SubqueryType

	// OuterColumn is the column from outer query (for correlated subqueries)
	OuterColumn *ColumnExpr
}

func (e *SubqueryExpr) String() string {
	return fmt.Sprintf("(%s SUBQUERY)", e.Type)
}

func (e *SubqueryExpr) UsedTables() Bitmask {
	var mask Bitmask
	if e.Query != nil {
		mask = e.Query.UsedTables()
	}
	if e.OuterColumn != nil {
		mask |= e.OuterColumn.UsedTables()
	}
	return mask
}
