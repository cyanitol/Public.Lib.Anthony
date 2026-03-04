// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package driver

import (
	"database/sql/driver"
	"fmt"
	"sort"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// compileCompoundSelect compiles a compound SELECT (UNION, UNION ALL, INTERSECT, EXCEPT).
// It executes both sides, applies the set operation, then emits result rows.
func (s *Stmt) compileCompoundSelect(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(true)

	compound := stmt.Compound

	// Collect all leaf SELECTs and their operators in left-to-right order.
	// A chain like A UNION B INTERSECT C becomes [{UNION, A, B}, {INTERSECT, _, C}]
	ops, selects := flattenCompound(compound)

	// Extract ORDER BY and LIMIT/OFFSET from the compound statement BEFORE compiling
	// sub-SELECTs. In the parser, ORDER BY/LIMIT on a compound ends up attached to
	// the rightmost leaf SELECT, but they apply to the compound result, not to that
	// individual SELECT. We must strip them before compilation to avoid errors when
	// the ORDER BY references column names from the first SELECT.
	orderBy, limit, offset := extractCompoundOrderByLimit(compound)

	// Execute each leaf SELECT and collect rows.
	allResults := make([][][]interface{}, len(selects))
	var numCols int
	for i, sel := range selects {
		subVM := vdbe.New()
		subVM.Ctx = vm.Ctx
		compiled, err := s.compileSelect(subVM, sel, args)
		if err != nil {
			return nil, fmt.Errorf("compound SELECT part %d: %w", i+1, err)
		}

		// Determine column count from the compiled VM's result columns
		cols := len(compiled.ResultCols)
		if i == 0 {
			numCols = cols
			vm.ResultCols = compiled.ResultCols
		} else if cols != numCols {
			return nil, fmt.Errorf("SELECTs to the left and right of %s do not have the same number of result columns", ops[i-1].String())
		}

		rows, err := s.collectRows(compiled, cols, fmt.Sprintf("compound part %d", i+1))
		if err != nil {
			return nil, err
		}
		allResults[i] = rows
	}

	// Apply set operations left to right.
	result := allResults[0]
	for i, op := range ops {
		right := allResults[i+1]
		result = applySetOperation(op, result, right, numCols)
	}

	// Apply ORDER BY to the in-memory result set.
	if len(orderBy) > 0 {
		sortCompoundRows(result, orderBy, numCols, vm.ResultCols)
	}

	// Apply LIMIT/OFFSET.
	result = applyLimitOffset(result, limit, offset)

	// Emit bytecode to return the collected rows.
	return emitCompoundResult(vm, result, numCols)
}

// flattenCompound walks the compound tree and returns operators and leaf SELECTs
// in left-to-right order. For "A UNION B INTERSECT C":
//   ops = [UNION, INTERSECT]
//   selects = [A, B, C]
func flattenCompound(c *parser.CompoundSelect) ([]parser.CompoundOp, []*parser.SelectStmt) {
	var ops []parser.CompoundOp
	var selects []*parser.SelectStmt

	// Walk left side
	if c.Left.Compound != nil {
		leftOps, leftSels := flattenCompound(c.Left.Compound)
		ops = append(ops, leftOps...)
		selects = append(selects, leftSels...)
	} else {
		selects = append(selects, c.Left)
	}

	ops = append(ops, c.Op)

	// Walk right side
	if c.Right.Compound != nil {
		rightOps, rightSels := flattenCompound(c.Right.Compound)
		ops = append(ops, rightOps...)
		selects = append(selects, rightSels...)
	} else {
		selects = append(selects, c.Right)
	}

	return ops, selects
}

// extractCompoundOrderByLimit extracts ORDER BY, LIMIT, and OFFSET from the
// rightmost leaf of the compound tree. In the parser, clauses after the last
// SELECT in a compound chain are attached to that last SELECT.
func extractCompoundOrderByLimit(c *parser.CompoundSelect) ([]parser.OrderingTerm, parser.Expression, parser.Expression) {
	// Walk to the rightmost leaf
	right := c.Right
	for right.Compound != nil {
		right = right.Compound.Right
	}

	orderBy := right.OrderBy
	limit := right.Limit
	offset := right.Offset

	// Clear them from the leaf so they don't affect the sub-SELECT execution
	right.OrderBy = nil
	right.Limit = nil
	right.Offset = nil

	return orderBy, limit, offset
}

// rowKey creates a string key for a row, used for deduplication and set operations.
func rowKey(row []interface{}) string {
	key := ""
	for i, v := range row {
		if i > 0 {
			key += "\x00"
		}
		if v == nil {
			key += "\x01NULL"
		} else {
			key += fmt.Sprintf("%T:%v", v, v)
		}
	}
	return key
}

// applySetOperation applies a single set operation between left and right row sets.
func applySetOperation(op parser.CompoundOp, left, right [][]interface{}, numCols int) [][]interface{} {
	switch op {
	case parser.CompoundUnionAll:
		return append(left, right...)

	case parser.CompoundUnion:
		// Concatenate then deduplicate
		combined := append(left, right...)
		return deduplicateRows(combined)

	case parser.CompoundIntersect:
		return intersectRows(left, right)

	case parser.CompoundExcept:
		return exceptRows(left, right)

	default:
		return left
	}
}

// deduplicateRows removes duplicate rows, preserving order of first occurrence.
func deduplicateRows(rows [][]interface{}) [][]interface{} {
	seen := make(map[string]bool)
	var result [][]interface{}
	for _, row := range rows {
		k := rowKey(row)
		if !seen[k] {
			seen[k] = true
			result = append(result, row)
		}
	}
	return result
}

// intersectRows returns rows that appear in both left and right (deduplicated).
func intersectRows(left, right [][]interface{}) [][]interface{} {
	rightSet := make(map[string]bool)
	for _, row := range right {
		rightSet[rowKey(row)] = true
	}

	seen := make(map[string]bool)
	var result [][]interface{}
	for _, row := range left {
		k := rowKey(row)
		if rightSet[k] && !seen[k] {
			seen[k] = true
			result = append(result, row)
		}
	}
	return result
}

// exceptRows returns rows in left that do not appear in right (deduplicated).
func exceptRows(left, right [][]interface{}) [][]interface{} {
	rightSet := make(map[string]bool)
	for _, row := range right {
		rightSet[rowKey(row)] = true
	}

	seen := make(map[string]bool)
	var result [][]interface{}
	for _, row := range left {
		k := rowKey(row)
		if !rightSet[k] && !seen[k] {
			seen[k] = true
			result = append(result, row)
		}
	}
	return result
}

// orderSpec defines a column to sort by and its direction.
type orderSpec struct {
	colIdx int
	desc   bool
}

// sortCompoundRows sorts the in-memory result set according to ORDER BY terms.
func sortCompoundRows(rows [][]interface{}, orderBy []parser.OrderingTerm, numCols int, colNames []string) {
	if len(rows) == 0 || len(orderBy) == 0 {
		return
	}

	specs := buildOrderSpecs(orderBy, numCols, colNames)
	sort.SliceStable(rows, func(i, j int) bool {
		return compareCompoundRows(rows[i], rows[j], specs)
	})
}

// buildOrderSpecs converts ORDER BY terms into orderSpec structures.
func buildOrderSpecs(orderBy []parser.OrderingTerm, numCols int, colNames []string) []orderSpec {
	specs := make([]orderSpec, 0, len(orderBy))
	for _, term := range orderBy {
		colIdx := resolveOrderByColumn(term, numCols, colNames)
		specs = append(specs, orderSpec{colIdx: colIdx, desc: !term.Asc})
	}
	return specs
}

// resolveOrderByColumn determines the column index for an ORDER BY term.
func resolveOrderByColumn(term parser.OrderingTerm, numCols int, colNames []string) int {
	baseExpr := extractBaseExpr(term.Expr)

	if colIdx := resolveIdentExpr(baseExpr, colNames); colIdx >= 0 {
		return colIdx
	}
	if colIdx := resolveLiteralExpr(baseExpr, numCols); colIdx >= 0 {
		return colIdx
	}
	return 0 // Default to first column
}

// extractBaseExpr unwraps a COLLATE expression if present.
func extractBaseExpr(expr parser.Expression) parser.Expression {
	if collateExpr, ok := expr.(*parser.CollateExpr); ok {
		return collateExpr.Expr
	}
	return expr
}

// resolveIdentExpr resolves a column name identifier to its index.
func resolveIdentExpr(expr parser.Expression, colNames []string) int {
	ident, ok := expr.(*parser.IdentExpr)
	if !ok {
		return -1
	}
	for j, name := range colNames {
		if name == ident.Name {
			return j
		}
	}
	return -1
}

// resolveLiteralExpr resolves a literal integer (1-based column index).
func resolveLiteralExpr(expr parser.Expression, numCols int) int {
	lit, ok := expr.(*parser.LiteralExpr)
	if !ok || lit.Type != parser.LiteralInteger {
		return -1
	}
	var idx int64
	if _, err := fmt.Sscanf(lit.Value, "%d", &idx); err == nil && idx >= 1 && int(idx) <= numCols {
		return int(idx) - 1
	}
	return -1
}

// compareCompoundRows compares two rows according to orderSpecs.
func compareCompoundRows(row1, row2 []interface{}, specs []orderSpec) bool {
	for _, spec := range specs {
		ci := spec.colIdx
		if ci >= len(row1) || ci >= len(row2) {
			continue
		}
		cmp := cmpCompoundValues(row1[ci], row2[ci])
		if cmp == 0 {
			continue
		}
		if spec.desc {
			return cmp > 0
		}
		return cmp < 0
	}
	return false
}

// cmpCompoundValues compares two interface{} values using SQLite-like ordering.
// NULL < integers < floats < strings < blobs
func cmpCompoundValues(a, b interface{}) int {
	// Handle NULLs first with early returns
	if cmp, handled := cmpNulls(a, b); handled {
		return cmp
	}

	// Compare different types by their type order
	if cmp, handled := cmpDifferentTypes(a, b); handled {
		return cmp
	}

	// Same type comparison - dispatch to type-specific comparers
	return cmpSameType(a, b)
}

// cmpNulls handles NULL comparison. Returns (comparison, true) if either value is NULL.
func cmpNulls(a, b interface{}) (int, bool) {
	if a == nil && b == nil {
		return 0, true
	}
	if a == nil {
		return -1, true
	}
	if b == nil {
		return 1, true
	}
	return 0, false
}

// cmpDifferentTypes compares values of different types by type order.
// Returns (comparison, true) if types differ, (0, false) if types are the same.
func cmpDifferentTypes(a, b interface{}) (int, bool) {
	aOrder := typeOrder(a)
	bOrder := typeOrder(b)
	if aOrder == bOrder {
		return 0, false
	}
	if aOrder < bOrder {
		return -1, true
	}
	return 1, true
}

// cmpSameType dispatches to type-specific comparison functions.
func cmpSameType(a, b interface{}) int {
	switch av := a.(type) {
	case int64:
		return cmpIntegers(av, b.(int64))
	case float64:
		return cmpFloats(av, b.(float64))
	case string:
		return cmpStrings(av, b.(string))
	case []byte:
		return cmpBytes(av, b.([]byte))
	}
	return 0
}

// cmpIntegers compares two int64 values.
func cmpIntegers(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// cmpFloats compares two float64 values.
func cmpFloats(a, b float64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// cmpStrings compares two string values.
func cmpStrings(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// cmpBytes compares two byte slices lexicographically.
func cmpBytes(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// typeOrder returns a sort-order rank for a value type.
// SQLite ordering: NULL=0, INTEGER/REAL=1, TEXT=2, BLOB=3
func typeOrder(v interface{}) int {
	switch v.(type) {
	case nil:
		return 0
	case int64:
		return 1
	case float64:
		return 1
	case string:
		return 2
	case []byte:
		return 3
	default:
		return 4
	}
}

// applyLimitOffset applies LIMIT and OFFSET to the result set.
func applyLimitOffset(rows [][]interface{}, limitExpr, offsetExpr parser.Expression) [][]interface{} {
	offset := 0
	limit := -1 // -1 means no limit

	if offsetExpr != nil {
		if lit, ok := offsetExpr.(*parser.LiteralExpr); ok {
			var v int64
			if _, err := fmt.Sscanf(lit.Value, "%d", &v); err == nil && v > 0 {
				offset = int(v)
			}
		}
	}

	if limitExpr != nil {
		if lit, ok := limitExpr.(*parser.LiteralExpr); ok {
			var v int64
			if _, err := fmt.Sscanf(lit.Value, "%d", &v); err == nil {
				limit = int(v)
			}
		}
	}

	// Apply offset
	if offset > 0 {
		if offset >= len(rows) {
			return nil
		}
		rows = rows[offset:]
	}

	// Apply limit
	if limit >= 0 && limit < len(rows) {
		rows = rows[:limit]
	}

	return rows
}

// emitCompoundResult generates VDBE bytecode that returns the pre-computed result rows.
func emitCompoundResult(vm *vdbe.VDBE, rows [][]interface{}, numCols int) (*vdbe.VDBE, error) {
	vm.AllocMemory(numCols + 10)

	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	for _, row := range rows {
		for i := 0; i < numCols; i++ {
			if i < len(row) {
				emitLoadValue(vm, row[i], i)
			} else {
				vm.AddOp(vdbe.OpNull, 0, i, 0)
			}
		}
		vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)
	}

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// emitLoadValue generates bytecode to load a value into a register.
func emitLoadValue(vm *vdbe.VDBE, val interface{}, reg int) {
	switch v := val.(type) {
	case nil:
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
	case int64:
		vm.AddOp(vdbe.OpInteger, int(v), reg, 0)
	case float64:
		vm.AddOpWithP4Real(vdbe.OpReal, 0, reg, 0, v)
	case string:
		vm.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, v)
	case []byte:
		vm.AddOpWithP4Blob(vdbe.OpBlob, len(v), reg, 0, v)
	default:
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
	}
}
