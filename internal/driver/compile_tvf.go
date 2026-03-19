// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"
	"fmt"
	"sort"
	"strings"

	"github.com/cyanitol/Public.Lib.Anthony/internal/functions"
	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/schema"
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

	// Apply WHERE filter before projection
	if stmt.Where != nil {
		rows = filterTVFRows(rows, stmt.Where, tvfCols)
	}

	outCols, colIndices := resolveTVFColumns(stmt.Columns, tvfCols)

	// Project rows to output columns
	projected := projectTVFRows(rows, colIndices)

	// Apply DISTINCT before ORDER BY
	if stmt.Distinct {
		projected = deduplicateTVFRows(projected)
	}

	// Apply ORDER BY
	if len(stmt.OrderBy) > 0 {
		sortTVFRows(projected, stmt.OrderBy, outCols)
	}

	return emitTVFProjectedBytecode(vm, projected, outCols)
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

// materializeTVFAsEphemeral executes a TVF, materializes its rows into an
// ephemeral table, and rewrites stmt.From so the aggregate pipeline can
// process the results. The caller should fall through to routeSpecializedSelect.
func (s *Stmt) materializeTVFAsEphemeral(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) error {
	ref := &stmt.From.Tables[0]
	tvf := s.lookupTVF(ref.TableName)
	if tvf == nil {
		return fmt.Errorf("table-valued function not found: %s", ref.TableName)
	}

	funcArgs, err := evalTVFArgs(ref.FuncArgs, args)
	if err != nil {
		return fmt.Errorf("%s: %w", ref.TableName, err)
	}

	rows, err := tvf.Open(funcArgs)
	if err != nil {
		return fmt.Errorf("%s: %w", ref.TableName, err)
	}

	tvfCols := tvf.Columns()
	tempTable, cursorNum := s.createTVFTempTable(ref.TableName, tvfCols, vm)
	s.conn.schema.AddTableDirect(tempTable)
	emitTVFMaterialize(vm, rows, tvfCols, cursorNum)
	rewriteFromForTVF(ref, tempTable.Name)
	return nil
}

// createTVFTempTable creates a temporary schema table and allocates a cursor
// for the ephemeral TVF materialization.
func (s *Stmt) createTVFTempTable(tvfName string, tvfCols []string, vm *vdbe.VDBE) (*schema.Table, int) {
	tableName := fmt.Sprintf("_tvf_%s", tvfName)
	columns := make([]*schema.Column, len(tvfCols))
	for i, name := range tvfCols {
		columns[i] = &schema.Column{Name: name, Type: "ANY"}
	}
	cursorNum := len(vm.Cursors)
	vm.AllocCursors(cursorNum + 1)
	table := &schema.Table{
		Name:     tableName,
		Columns:  columns,
		RootPage: uint32(cursorNum),
		Temp:     true,
	}
	return table, cursorNum
}

// emitTVFMaterialize emits bytecode to open an ephemeral table and insert all
// TVF result rows into it.
func emitTVFMaterialize(vm *vdbe.VDBE, rows [][]functions.Value, tvfCols []string, cursorNum int) {
	numCols := len(tvfCols)
	vm.AllocMemory(numCols + 20)
	vm.AddOp(vdbe.OpOpenEphemeral, cursorNum, numCols, 0)

	recordReg := numCols // register for MakeRecord output
	for _, row := range rows {
		for i := 0; i < numCols; i++ {
			if i < len(row) {
				emitFuncValue(vm, row[i], i)
			} else {
				vm.AddOp(vdbe.OpNull, 0, i, 0)
			}
		}
		vm.AddOp(vdbe.OpMakeRecord, 0, numCols, recordReg)
		vm.AddOp(vdbe.OpInsert, cursorNum, recordReg, 0)
	}
}

// rewriteFromForTVF rewrites a table reference to point at the materialized
// temp table instead of the TVF call.
func rewriteFromForTVF(ref *parser.TableOrSubquery, tempName string) {
	ref.TableName = tempName
	ref.FuncArgs = nil
}

// projectTVFRows projects full TVF rows down to selected output columns.
func projectTVFRows(rows [][]functions.Value, colIndices []int) [][]functions.Value {
	projected := make([][]functions.Value, len(rows))
	for i, row := range rows {
		out := make([]functions.Value, len(colIndices))
		for j, srcIdx := range colIndices {
			if srcIdx >= 0 && srcIdx < len(row) {
				out[j] = row[srcIdx]
			} else {
				out[j] = functions.NewNullValue()
			}
		}
		projected[i] = out
	}
	return projected
}

// deduplicateTVFRows removes duplicate rows using string-key dedup.
func deduplicateTVFRows(rows [][]functions.Value) [][]functions.Value {
	seen := make(map[string]struct{}, len(rows))
	result := make([][]functions.Value, 0, len(rows))
	for _, row := range rows {
		key := tvfRowKey(row)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, row)
	}
	return result
}

// tvfRowKey builds a string key for a row for deduplication.
func tvfRowKey(row []functions.Value) string {
	parts := make([]string, len(row))
	for i, v := range row {
		if v == nil || v.IsNull() {
			parts[i] = "\x00null"
		} else {
			parts[i] = fmt.Sprintf("%d:%s", v.Type(), v.AsString())
		}
	}
	return strings.Join(parts, "\x01")
}

// tvfSortKey describes one ORDER BY column for TVF row sorting.
type tvfSortKey struct {
	colIdx int
	desc   bool
}

// sortTVFRows sorts projected rows according to ORDER BY terms.
func sortTVFRows(rows [][]functions.Value, orderBy []parser.OrderingTerm, outCols []string) {
	keys := make([]tvfSortKey, 0, len(orderBy))
	for _, term := range orderBy {
		idx := resolveTVFOrderByCol(term.Expr, outCols)
		if idx < 0 {
			continue
		}
		keys = append(keys, tvfSortKey{colIdx: idx, desc: !term.Asc})
	}
	sort.SliceStable(rows, func(i, j int) bool {
		return tvfRowLess(rows[i], rows[j], keys)
	})
}

// tvfRowLess returns true if row a should sort before row b.
func tvfRowLess(a, b []functions.Value, keys []tvfSortKey) bool {
	for _, k := range keys {
		cmp := compareFuncValues(a[k.colIdx], b[k.colIdx])
		if cmp == 0 {
			continue
		}
		if k.desc {
			return cmp > 0
		}
		return cmp < 0
	}
	return false
}

// resolveTVFOrderByCol maps an ORDER BY expression to an output column index.
func resolveTVFOrderByCol(e parser.Expression, outCols []string) int {
	if lit, ok := e.(*parser.LiteralExpr); ok {
		var n int
		if _, err := fmt.Sscanf(lit.Value, "%d", &n); err == nil && n >= 1 && n <= len(outCols) {
			return n - 1
		}
	}
	if ident, ok := e.(*parser.IdentExpr); ok {
		lower := strings.ToLower(ident.Name)
		for i, c := range outCols {
			if strings.ToLower(c) == lower {
				return i
			}
		}
	}
	return -1
}

// compareFuncValues compares two functions.Value using SQLite ordering:
// NULL < integers/floats < text < blob.
func compareFuncValues(a, b functions.Value) int {
	aNil := a == nil || a.IsNull()
	bNil := b == nil || b.IsNull()
	if aNil && bNil {
		return 0
	}
	if aNil {
		return -1
	}
	if bNil {
		return 1
	}
	// Compare by type affinity, then by value
	aType, bType := a.Type(), b.Type()
	if aType != bType {
		return int(aType) - int(bType)
	}
	return compareByType(a, b, aType)
}

// compareByType compares two non-null values of the same type.
func compareByType(a, b functions.Value, typ functions.ValueType) int {
	switch typ {
	case functions.TypeInteger:
		ai, bi := a.AsInt64(), b.AsInt64()
		if ai < bi {
			return -1
		}
		if ai > bi {
			return 1
		}
		return 0
	case functions.TypeFloat:
		af, bf := a.AsFloat64(), b.AsFloat64()
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	default:
		return strings.Compare(a.AsString(), b.AsString())
	}
}

// emitTVFProjectedBytecode generates VDBE bytecode for pre-projected TVF rows.
func emitTVFProjectedBytecode(vm *vdbe.VDBE, rows [][]functions.Value, colNames []string) (*vdbe.VDBE, error) {
	numCols := len(colNames)
	vm.AllocMemory(numCols + 10)
	vm.ResultCols = colNames

	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	for _, row := range rows {
		for i := 0; i < numCols; i++ {
			if i < len(row) {
				emitFuncValue(vm, row[i], i)
			} else {
				vm.AddOp(vdbe.OpNull, 0, i, 0)
			}
		}
		vm.AddOp(vdbe.OpResultRow, 0, numCols, 0)
	}

	vm.AddOp(vdbe.OpHalt, 0, 0, 0)
	return vm, nil
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

// filterTVFRows filters TVF rows by evaluating a WHERE expression.
func filterTVFRows(rows [][]functions.Value, where parser.Expression, tvfCols []string) [][]functions.Value {
	var result [][]functions.Value
	for _, row := range rows {
		if evalTVFWhere(where, row, tvfCols) {
			result = append(result, row)
		}
	}
	return result
}

// evalTVFWhere evaluates a WHERE expression against a single TVF row.
func evalTVFWhere(expr parser.Expression, row []functions.Value, cols []string) bool {
	switch e := expr.(type) {
	case *parser.UnaryExpr:
		return evalTVFUnary(e, row, cols)
	case *parser.BinaryExpr:
		return evalTVFBinary(e, row, cols)
	default:
		return true // conservative: include row for unhandled expressions
	}
}

// evalTVFUnary evaluates a unary expression (IS NULL, IS NOT NULL) against a TVF row.
func evalTVFUnary(e *parser.UnaryExpr, row []functions.Value, cols []string) bool {
	val := resolveTVFValue(e.Expr, row, cols)
	isNull := val == nil || val.IsNull()
	switch e.Op {
	case parser.OpIsNull:
		return isNull
	case parser.OpNotNull:
		return !isNull
	default:
		return true
	}
}

// evalTVFBinary evaluates a binary expression against a TVF row.
func evalTVFBinary(e *parser.BinaryExpr, row []functions.Value, cols []string) bool {
	switch e.Op {
	case parser.OpAnd:
		return evalTVFWhere(e.Left, row, cols) && evalTVFWhere(e.Right, row, cols)
	case parser.OpOr:
		return evalTVFWhere(e.Left, row, cols) || evalTVFWhere(e.Right, row, cols)
	default:
		left := resolveTVFValue(e.Left, row, cols)
		right := resolveTVFValue(e.Right, row, cols)
		return evalTVFComparison(e.Op, left, right)
	}
}

// evalTVFComparison evaluates a comparison between two TVF values.
func evalTVFComparison(op parser.BinaryOp, left, right functions.Value) bool {
	cmp := compareFuncValues(left, right)
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

// resolveTVFValue resolves an expression to a functions.Value from a TVF row.
func resolveTVFValue(expr parser.Expression, row []functions.Value, cols []string) functions.Value {
	switch e := expr.(type) {
	case *parser.IdentExpr:
		idx := findTVFColIndex(e.Name, cols)
		if idx >= 0 && idx < len(row) {
			return row[idx]
		}
		return functions.NewNullValue()
	case *parser.LiteralExpr:
		return literalToFuncValue(e)
	default:
		return functions.NewNullValue()
	}
}
