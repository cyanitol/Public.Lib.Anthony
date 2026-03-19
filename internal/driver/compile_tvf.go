// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/functions"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// hasTableValuedFunction checks if a SELECT has a table-valued function in FROM.
func (s *Stmt) hasTableValuedFunction(stmt *parser.SelectStmt) bool {
	if stmt.From == nil || len(stmt.From.Tables) == 0 {
		return false
	}
	ref := &stmt.From.Tables[0]
	if ref.FuncArgs == nil {
		return false
	}
	return s.lookupTVF(ref.TableName) != nil
}

// lookupTVF finds a table-valued function by name in the function registry.
func (s *Stmt) lookupTVF(name string) functions.TableValuedFunction {
	fn, ok := s.conn.funcReg.Lookup(strings.ToLower(name))
	if !ok {
		return nil
	}
	tvf, ok := fn.(functions.TableValuedFunction)
	if !ok {
		return nil
	}
	return tvf
}

// compileSelectWithTVF compiles a SELECT from a table-valued function.
func (s *Stmt) compileSelectWithTVF(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	vm.SetReadOnly(true)

	ref := &stmt.From.Tables[0]
	tvf := s.lookupTVF(ref.TableName)
	if tvf == nil {
		return nil, fmt.Errorf("table-valued function not found: %s", ref.TableName)
	}

	// Evaluate function arguments from AST literals
	funcArgs, err := evalTVFArgs(ref.FuncArgs, args)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ref.TableName, err)
	}

	// Execute the TVF to get all result rows
	rows, err := tvf.Open(funcArgs)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ref.TableName, err)
	}

	// Resolve output columns
	tvfCols := tvf.Columns()
	outCols, colIndices := resolveTVFColumns(stmt.Columns, tvfCols)

	return emitTVFBytecode(vm, rows, outCols, colIndices)
}

// evalTVFArgs converts parser expressions to functions.Value arguments.
func evalTVFArgs(exprs []parser.Expression, args []driver.NamedValue) ([]functions.Value, error) {
	result := make([]functions.Value, len(exprs))
	for i, e := range exprs {
		val, err := evalLiteralExpr(e, args)
		if err != nil {
			return nil, fmt.Errorf("argument %d: %w", i+1, err)
		}
		result[i] = val
	}
	return result, nil
}

// evalLiteralExpr evaluates a parser expression to a functions.Value.
func evalLiteralExpr(e parser.Expression, args []driver.NamedValue) (functions.Value, error) {
	switch lit := e.(type) {
	case *parser.LiteralExpr:
		return literalToFuncValue(lit), nil
	case *parser.VariableExpr:
		return variableToFuncValue(lit, args)
	default:
		return nil, fmt.Errorf("unsupported expression type in TVF argument")
	}
}

// literalToFuncValue converts a parser LiteralExpr to a functions.Value.
func literalToFuncValue(lit *parser.LiteralExpr) functions.Value {
	switch lit.Type {
	case parser.LiteralString:
		return functions.NewTextValue(lit.Value)
	case parser.LiteralInteger:
		var n int64
		fmt.Sscanf(lit.Value, "%d", &n)
		return functions.NewIntValue(n)
	case parser.LiteralFloat:
		var f float64
		fmt.Sscanf(lit.Value, "%f", &f)
		return functions.NewFloatValue(f)
	default:
		return functions.NewNullValue()
	}
}

// variableToFuncValue resolves a variable/bind parameter to a functions.Value.
func variableToFuncValue(v *parser.VariableExpr, args []driver.NamedValue) (functions.Value, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("no arguments provided for bind parameter")
	}
	// Match by name if available, else use first positional arg
	for _, a := range args {
		if v.Name != "" && a.Name == v.Name {
			return driverValueToFuncValue(a.Value), nil
		}
	}
	if len(args) > 0 {
		return driverValueToFuncValue(args[0].Value), nil
	}
	return nil, fmt.Errorf("bind parameter not found")
}

// driverValueToFuncValue converts a driver.Value to a functions.Value.
func driverValueToFuncValue(v interface{}) functions.Value {
	switch val := v.(type) {
	case int64:
		return functions.NewIntValue(val)
	case float64:
		return functions.NewFloatValue(val)
	case string:
		return functions.NewTextValue(val)
	case []byte:
		return functions.NewBlobValue(val)
	case nil:
		return functions.NewNullValue()
	default:
		return functions.NewTextValue(fmt.Sprintf("%v", val))
	}
}

// resolveTVFColumns maps SELECT columns to TVF output column indices.
// Returns the output column names and indices into the TVF row.
func resolveTVFColumns(selectCols []parser.ResultColumn, tvfCols []string) ([]string, []int) {
	// Handle SELECT *
	if len(selectCols) == 1 && selectCols[0].Star {
		indices := make([]int, len(tvfCols))
		for i := range indices {
			indices[i] = i
		}
		return tvfCols, indices
	}

	names := make([]string, 0, len(selectCols))
	indices := make([]int, 0, len(selectCols))
	for _, col := range selectCols {
		name := extractColName(col)
		idx := findTVFColIndex(name, tvfCols)
		names = append(names, colDisplayName(col, name))
		indices = append(indices, idx)
	}
	return names, indices
}

// extractColName extracts the column name from a result column expression.
func extractColName(col parser.ResultColumn) string {
	if ident, ok := col.Expr.(*parser.IdentExpr); ok {
		return ident.Name
	}
	return ""
}

// colDisplayName returns the display name for a column.
func colDisplayName(col parser.ResultColumn, fallback string) string {
	if col.Alias != "" {
		return col.Alias
	}
	return fallback
}

// findTVFColIndex finds the index of a column name in TVF columns.
func findTVFColIndex(name string, tvfCols []string) int {
	lower := strings.ToLower(name)
	for i, c := range tvfCols {
		if strings.ToLower(c) == lower {
			return i
		}
	}
	return -1
}

// emitTVFBytecode generates VDBE bytecode that outputs pre-computed TVF rows.
func emitTVFBytecode(vm *vdbe.VDBE, rows [][]functions.Value, colNames []string, colIndices []int) (*vdbe.VDBE, error) {
	numCols := len(colNames)
	vm.AllocMemory(numCols + 10)
	vm.ResultCols = colNames

	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	for _, row := range rows {
		emitTVFRow(vm, row, colIndices, numCols)
	}

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
}

// emitTVFRow emits VDBE instructions for a single TVF result row.
func emitTVFRow(vm *vdbe.VDBE, row []functions.Value, colIndices []int, numCols int) {
	for outIdx, srcIdx := range colIndices {
		if srcIdx < 0 || srcIdx >= len(row) {
			vm.AddOp(vdbe.OpNull, 0, outIdx, 0)
			continue
		}
		emitFuncValue(vm, row[srcIdx], outIdx)
	}
	vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)
}

// emitFuncValue emits a VDBE instruction to load a functions.Value into a register.
func emitFuncValue(vm *vdbe.VDBE, val functions.Value, reg int) {
	if val == nil || val.IsNull() {
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
		return
	}

	sv, ok := val.(*functions.SimpleValue)
	if !ok {
		vm.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, val.AsString())
		return
	}

	switch sv.Type() {
	case functions.TypeInteger:
		emitIntValue(vm, sv.AsInt64(), reg)
	case functions.TypeFloat:
		vm.AddOpWithP4Real(vdbe.OpReal, 0, reg, 0, sv.AsFloat64())
	case functions.TypeText:
		vm.AddOpWithP4Str(vdbe.OpString8, 0, reg, 0, sv.AsString())
	default:
		vm.AddOp(vdbe.OpNull, 0, reg, 0)
	}
}

// emitIntValue emits an integer value into a register.
// Uses OpInteger for small values, OpInt64 for large ones.
func emitIntValue(vm *vdbe.VDBE, n int64, reg int) {
	if n >= -2147483648 && n <= 2147483647 {
		vm.AddOp(vdbe.OpInteger, int(n), reg, 0)
	} else {
		vm.AddOpWithP4Int64(vdbe.OpInt64, 0, reg, 0, n)
	}
}
