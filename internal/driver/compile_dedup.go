// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"fmt"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// findColIndexCI finds a column by name (case-insensitive) in a string slice.
// Returns -1 if not found.
func findColIndexCI(name string, cols []string) int {
	lower := strings.ToLower(name)
	for i, c := range cols {
		if strings.ToLower(c) == lower {
			return i
		}
	}
	return -1
}

// extractResultColName extracts the column name from a ResultColumn expression.
// Returns "" if the expression is not an IdentExpr.
func extractResultColName(col parser.ResultColumn) string {
	if e, ok := col.Expr.(*parser.IdentExpr); ok {
		return e.Name
	}
	return ""
}

// resolveColumnMapping maps SELECT columns to source column indices.
// For SELECT *, returns all source columns. For named columns, looks up
// each column in sourceCols by case-insensitive name. The displayName
// callback customises the output name for non-star columns; if nil the
// source column name (or alias) is used.
func resolveColumnMapping(selectCols []parser.ResultColumn, sourceCols []string,
	displayName func(parser.ResultColumn, string, int) string) ([]string, []int) {

	if len(selectCols) == 1 && selectCols[0].Star {
		indices := make([]int, len(sourceCols))
		for i := range indices {
			indices[i] = i
		}
		return sourceCols, indices
	}

	names := make([]string, 0, len(selectCols))
	indices := make([]int, 0, len(selectCols))
	for _, col := range selectCols {
		colName := extractResultColName(col)
		idx := findColIndexCI(colName, sourceCols)
		if displayName != nil {
			names = append(names, displayName(col, colName, idx))
		} else {
			if col.Alias != "" {
				names = append(names, col.Alias)
			} else {
				names = append(names, colName)
			}
		}
		indices = append(indices, idx)
	}
	return names, indices
}

// compareOpResult evaluates a comparison operator against an integer cmp value.
// Returns true if the comparison holds; returns true for unrecognised operators
// (conservative: include row).
func compareOpResult(op parser.BinaryOp, cmp int) bool {
	switch op {
	case parser.OpEq:
		return cmp == 0
	case parser.OpNe:
		return cmp != 0
	case parser.OpLt:
		return cmp < 0
	case parser.OpGt:
		return cmp > 0
	case parser.OpLe:
		return cmp <= 0
	case parser.OpGe:
		return cmp >= 0
	default:
		return true
	}
}

// emitInterfaceRows generates VDBE bytecode for pre-computed []interface{} rows.
// Emits OpInit, row values via emitInterfaceValue, OpResultRow per row, and OpHalt.
func emitInterfaceRows(vm *vdbe.VDBE, rows [][]interface{}, colNames []string) (*vdbe.VDBE, error) {
	numCols := len(colNames)
	vm.AllocMemory(numCols + 10)
	vm.ResultCols = colNames

	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	for _, row := range rows {
		for i := 0; i < numCols; i++ {
			if i < len(row) {
				emitInterfaceValue(vm, row[i], i)
			} else {
				vm.AddOp(vdbe.OpNull, 0, i, 0)
			}
		}
		vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)
	}

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// interfaceRowKey builds a string key for a []interface{} row for deduplication.
func interfaceRowKey(row []interface{}) string {
	return fmt.Sprintf("%v", row)
}

// deduplicateRowsBy removes duplicate rows, preserving order of first
// occurrence.  The caller supplies a keyFunc that serialises a row into a
// comparable string.
func deduplicateRowsBy[T any](rows [][]T, keyFunc func([]T) string) [][]T {
	seen := make(map[string]struct{}, len(rows))
	result := make([][]T, 0, len(rows))
	for _, row := range rows {
		key := keyFunc(row)
		if _, dup := seen[key]; !dup {
			seen[key] = struct{}{}
			result = append(result, row)
		}
	}
	return result
}

// intersectRowsBy returns rows that appear in both left and right
// (deduplicated), preserving the order from left.
func intersectRowsBy[T any](left, right [][]T, keyFunc func([]T) string) [][]T {
	rightSet := make(map[string]struct{}, len(right))
	for _, row := range right {
		rightSet[keyFunc(row)] = struct{}{}
	}

	seen := make(map[string]struct{}, len(left))
	result := make([][]T, 0, len(left))
	for _, row := range left {
		k := keyFunc(row)
		if _, inRight := rightSet[k]; !inRight {
			continue
		}
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		result = append(result, row)
	}
	return result
}

// filterRowsBy returns the subset of rows for which predicate returns true.
func filterRowsBy[T any](rows [][]T, predicate func([]T) bool) [][]T {
	var result [][]T
	for _, row := range rows {
		if predicate(row) {
			result = append(result, row)
		}
	}
	return result
}

// compareNulls handles NULL comparison for sorting.
// Returns (result, true) if a NULL was involved, (0, false) otherwise.
// When nullsFirst is true, NULLs sort before non-NULLs (result -1 for aIsNil, +1 for bIsNil).
func compareNulls(aIsNil, bIsNil, nullsFirst bool) (int, bool) {
	if !aIsNil && !bIsNil {
		return 0, false
	}
	if aIsNil && bIsNil {
		return 0, true
	}
	if aIsNil {
		if nullsFirst {
			return -1, true
		}
		return 1, true
	}
	// bIsNil
	if nullsFirst {
		return 1, true
	}
	return -1, true
}

// exceptRowsBy returns rows in left that do not appear in right
// (deduplicated), preserving the order from left.
func exceptRowsBy[T any](left, right [][]T, keyFunc func([]T) string) [][]T {
	rightSet := make(map[string]struct{}, len(right))
	for _, row := range right {
		rightSet[keyFunc(row)] = struct{}{}
	}

	seen := make(map[string]struct{}, len(left))
	result := make([][]T, 0, len(left))
	for _, row := range left {
		k := keyFunc(row)
		if _, inRight := rightSet[k]; inRight {
			continue
		}
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		result = append(result, row)
	}
	return result
}
