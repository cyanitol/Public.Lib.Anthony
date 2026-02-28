package driver

import (
	"database/sql/driver"
	"fmt"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/parser"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/planner"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/schema"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/vdbe"
)

// compileSelectWithCTEs compiles a SELECT statement with a WITH clause (CTEs).
func (s *Stmt) compileSelectWithCTEs(vm *vdbe.VDBE, stmt *parser.SelectStmt, args []driver.NamedValue) (*vdbe.VDBE, error) {
	// Create CTE context from WITH clause
	cteCtx, err := planner.NewCTEContext(stmt.With)
	if err != nil {
		return nil, fmt.Errorf("failed to create CTE context: %w", err)
	}

	// Validate CTEs
	if err := cteCtx.ValidateCTEs(); err != nil {
		return nil, fmt.Errorf("CTE validation failed: %w", err)
	}

	vm.AddOp(vdbe.OpInit, 0, 0, 0)

	// Materialize CTEs in dependency order
	cteTempTables := make(map[string]*schema.Table)
	for _, cteName := range cteCtx.CTEOrder {
		def, exists := cteCtx.GetCTE(cteName)
		if !exists {
			return nil, fmt.Errorf("CTE not found in context: %s", cteName)
		}

		if def.IsRecursive {
			// Handle recursive CTE
			tempTable, err := s.compileRecursiveCTE(vm, cteName, def, cteCtx, cteTempTables, args)
			if err != nil {
				return nil, fmt.Errorf("failed to compile recursive CTE %s: %w", cteName, err)
			}
			cteTempTables[cteName] = tempTable
		} else {
			// Handle non-recursive CTE
			tempTable, err := s.compileNonRecursiveCTE(vm, cteName, def, cteCtx, cteTempTables, args)
			if err != nil {
				return nil, fmt.Errorf("failed to compile CTE %s: %w", cteName, err)
			}
			cteTempTables[cteName] = tempTable
		}
	}

	// Now compile the main query, replacing CTE references with temp tables
	mainStmt := s.rewriteSelectWithCTETables(stmt, cteTempTables)

	// Compile the main query (without the WITH clause)
	mainStmt.With = nil
	return s.compileSelect(vm, mainStmt, args)
}

// compileNonRecursiveCTE compiles a non-recursive CTE into a temporary table.
func (s *Stmt) compileNonRecursiveCTE(vm *vdbe.VDBE, cteName string, def *planner.CTEDefinition,
	cteCtx *planner.CTEContext, cteTempTables map[string]*schema.Table, args []driver.NamedValue) (*schema.Table, error) {

	// Create a temporary table to hold CTE results
	tempTableName := fmt.Sprintf("_cte_%s", cteName)
	tempTable := s.createCTETempTable(tempTableName, def)

	// Allocate a cursor for the ephemeral table
	cursorNum := len(vm.Cursors)
	vm.AllocCursors(cursorNum + 1)

	// Open an ephemeral table for this CTE
	vm.AddOp(vdbe.OpOpenEphemeral, cursorNum, len(tempTable.Columns), 0)

	// Store cursor number in temp table metadata for later reference
	tempTable.RootPage = uint32(cursorNum) // Use RootPage to store cursor number for ephemeral tables

	// Register the temp table in the schema so it can be found during compilation
	s.conn.schema.Tables[tempTableName] = tempTable

	// Rewrite the CTE's SELECT to use already-materialized CTEs
	cteSelect := s.rewriteSelectWithCTETables(def.Select, cteTempTables)

	// Generate bytecode to populate the ephemeral table
	if err := s.compileCTEPopulation(vm, cteSelect, cursorNum, len(tempTable.Columns), args); err != nil {
		return nil, fmt.Errorf("failed to compile CTE population: %w", err)
	}

	return tempTable, nil
}

// compileCTEPopulation generates bytecode to populate an ephemeral table with CTE results.
func (s *Stmt) compileCTEPopulation(vm *vdbe.VDBE, cteSelect *parser.SelectStmt, cursorNum int, numColumns int, args []driver.NamedValue) error {
	// Compile the CTE SELECT to generate rows
	cteVM := vdbe.New()
	cteVM.Ctx = vm.Ctx
	compiledCTE, err := s.compileSelect(cteVM, cteSelect, args)
	if err != nil {
		return fmt.Errorf("failed to compile CTE SELECT: %w", err)
	}

	// We need to inline the CTE bytecode into the main VM
	// The strategy is:
	// 1. Allocate cursors for the CTE (it might need cursors for its tables)
	// 2. Copy CTE bytecode, adjusting cursor numbers to avoid conflicts
	// 3. Replace OpResultRow with code to insert into ephemeral table
	// 4. Replace OpHalt with Noop to continue execution

	// Get cursor and register offsets for the CTE
	cteBaseCursor := len(vm.Cursors)
	cteCursorCount := len(compiledCTE.Cursors)
	if cteCursorCount > 0 {
		vm.AllocCursors(cteBaseCursor + cteCursorCount)
	}

	// Get register offset - CTE registers need to be shifted to avoid conflicts
	cteBaseRegister := len(vm.Mem)
	cteRegisterCount := len(compiledCTE.Mem)
	if cteRegisterCount > 0 {
		vm.AllocMemory(cteBaseRegister + cteRegisterCount)
	}

	// Allocate a register for record assembly
	recordReg := len(vm.Mem)
	vm.AllocMemory(recordReg + 1)

	// Mark where CTE bytecode starts
	cteStartAddr := vm.NumOps()

	// Copy CTE bytecode into main VM
	for _, instr := range compiledCTE.Program {
		newInstr := *instr

		// Adjust cursor and register numbers
		adjustedP1, adjustedP2, adjustedP3 := instr.P1, instr.P2, instr.P3

		// First adjust cursor numbers for cursor operations
		if needsCursorAdjustment(instr.Opcode) {
			adjustedP1 = instr.P1 + cteBaseCursor
		}

		// Then adjust register numbers
		adjustedP1, adjustedP2, adjustedP3 = adjustRegisterNumbers(
			instr.Opcode, adjustedP1, adjustedP2, adjustedP3, cteBaseRegister,
		)

		newInstr.P1 = adjustedP1
		newInstr.P2 = adjustedP2
		newInstr.P3 = adjustedP3

		// Handle special opcodes BEFORE adding the instruction
		switch instr.Opcode {
		case vdbe.OpResultRow:
			// Replace ResultRow with MakeRecord + Insert
			// OpResultRow was already adjusted: P1 = base register (adjusted), P2 = count
			newInstr.Opcode = vdbe.OpMakeRecord
			newInstr.P3 = recordReg // Output to recordReg

			// Add the Make Record instruction
			addr := vm.AddOp(newInstr.Opcode, newInstr.P1, newInstr.P2, newInstr.P3)
			vm.Program[addr].P4 = instr.P4
			vm.Program[addr].Comment = instr.Comment

			// Add Insert instruction to put record into ephemeral table
			vm.AddOp(vdbe.OpInsert, cursorNum, recordReg, 0)
			continue // Don't add the original OpResultRow

		case vdbe.OpHalt:
			// Replace Halt with Noop so execution continues
			newInstr.Opcode = vdbe.OpNoop
		}

		// Add the instruction (unless it was already added above)
		addr := vm.AddOp(newInstr.Opcode, newInstr.P1, newInstr.P2, newInstr.P3)
		vm.Program[addr].P4 = instr.P4
		vm.Program[addr].Comment = instr.Comment

		// Adjust jump targets AFTER adding the instruction
		switch instr.Opcode {
		case vdbe.OpGoto, vdbe.OpIf, vdbe.OpIfNot, vdbe.OpIfPos,
			vdbe.OpEq, vdbe.OpNe, vdbe.OpLt, vdbe.OpLe, vdbe.OpGt, vdbe.OpGe,
			vdbe.OpRewind, vdbe.OpNext, vdbe.OpPrev:
			// Adjust jump targets (P2) by adding the offset where CTE code starts
			if instr.P2 > 0 {
				vm.Program[addr].P2 = instr.P2 + cteStartAddr
			}
		}
	}

	return nil
}

// needsCursorAdjustment returns true if the opcode uses P1 as a cursor number.
func needsCursorAdjustment(op vdbe.Opcode) bool {
	switch op {
	case vdbe.OpOpenRead, vdbe.OpOpenWrite, vdbe.OpOpenEphemeral,
		vdbe.OpClose, vdbe.OpRewind, vdbe.OpNext, vdbe.OpPrev,
		vdbe.OpSeekGE, vdbe.OpSeekGT, vdbe.OpSeekLE, vdbe.OpSeekLT,
		vdbe.OpColumn, vdbe.OpInsert, vdbe.OpDelete:
		return true
	}
	return false
}

// adjustRegisterNumbers adjusts register numbers when inlining bytecode.
// Most opcodes use P1, P2, P3 for registers, but some use them for other purposes.
func adjustRegisterNumbers(op vdbe.Opcode, p1, p2, p3, baseReg int) (int, int, int) {
	// For opcodes that use cursors in P1, don't adjust P1
	if needsCursorAdjustment(op) {
		// P2 and P3 might still be registers
		switch op {
		case vdbe.OpColumn:
			// P1=cursor, P2=column, P3=dest register
			return p1, p2, p3 + baseReg
		case vdbe.OpInsert, vdbe.OpDelete:
			// P1=cursor, P2=data register, P3=key register (or 0)
			newP2 := p2 + baseReg
			newP3 := p3
			if p3 > 0 {
				newP3 = p3 + baseReg
			}
			return p1, newP2, newP3
		case vdbe.OpRewind, vdbe.OpNext, vdbe.OpPrev:
			// P1=cursor, P2=jump target, P3=unused
			return p1, p2, p3
		default:
			// For other cursor ops, only P1 is cursor
			return p1, p2, p3
		}
	}

	// For most other opcodes, adjust register parameters
	switch op {
	case vdbe.OpInteger, vdbe.OpReal, vdbe.OpString8, vdbe.OpBlob, vdbe.OpNull:
		// P1=value/size, P2=dest register, P3=unused
		return p1, p2 + baseReg, p3

	case vdbe.OpResultRow, vdbe.OpMakeRecord:
		// P1=start register, P2=count, P3=dest register (for MakeRecord)
		return p1 + baseReg, p2, p3 + baseReg

	case vdbe.OpCopy, vdbe.OpSCopy:
		// P1=src register, P2=dest register
		return p1 + baseReg, p2 + baseReg, p3

	case vdbe.OpAdd, vdbe.OpSubtract, vdbe.OpMultiply, vdbe.OpDivide, vdbe.OpRemainder,
		vdbe.OpConcat, vdbe.OpEq, vdbe.OpNe, vdbe.OpLt, vdbe.OpLe, vdbe.OpGt, vdbe.OpGe:
		// P1=left register, P2=right register or jump, P3=dest register
		// Note: comparison ops use P2 for jump target, not register
		switch op {
		case vdbe.OpEq, vdbe.OpNe, vdbe.OpLt, vdbe.OpLe, vdbe.OpGt, vdbe.OpGe:
			// P1=register, P2=jump target, P3=register
			return p1 + baseReg, p2, p3 + baseReg
		default:
			// Arithmetic ops: P1, P2, P3 all registers
			return p1 + baseReg, p2 + baseReg, p3 + baseReg
		}

	case vdbe.OpNot, vdbe.OpBitNot:
		// P1=src register, P2=dest register
		return p1 + baseReg, p2 + baseReg, p3

	case vdbe.OpGoto, vdbe.OpIf, vdbe.OpIfNot, vdbe.OpIfPos:
		// P1=register or unused, P2=jump target
		if op == vdbe.OpGoto {
			return p1, p2, p3
		}
		return p1 + baseReg, p2, p3

	case vdbe.OpInit, vdbe.OpHalt, vdbe.OpNoop:
		// No register adjustments needed
		return p1, p2, p3

	default:
		// Conservative default: don't adjust
		return p1, p2, p3
	}
}

// compileRecursiveCTE compiles a recursive CTE using iterative execution.
// compileRecursiveCTE compiles a recursive CTE using iterative execution.
func (s *Stmt) compileRecursiveCTE(vm *vdbe.VDBE, cteName string, def *planner.CTEDefinition,
	cteCtx *planner.CTEContext, cteTempTables map[string]*schema.Table, args []driver.NamedValue) (*schema.Table, error) {

	// Recursive CTEs must have a UNION structure
	if def.Select.Compound == nil {
		return nil, fmt.Errorf("recursive CTE %s must use UNION or UNION ALL", cteName)
	}

	compound := def.Select.Compound
	if compound.Op != parser.CompoundUnion && compound.Op != parser.CompoundUnionAll {
		return nil, fmt.Errorf("recursive CTE %s must use UNION or UNION ALL", cteName)
	}

	// Create temp tables for recursive iteration
	tempTableName := fmt.Sprintf("_cte_%s", cteName)
	currentTableName := fmt.Sprintf("_cte_%s_current", cteName)

	tempTable := s.createCTETempTable(tempTableName, def)
	currentTable := s.createCTETempTable(currentTableName, def)

	// Allocate cursors for both ephemeral tables
	resultCursor := len(vm.Cursors)
	vm.AllocCursors(resultCursor + 1)
	currentCursor := len(vm.Cursors)
	vm.AllocCursors(currentCursor + 1)

	// Open ephemeral tables
	numColumns := len(tempTable.Columns)
	vm.AddOp(vdbe.OpOpenEphemeral, resultCursor, numColumns, 0)
	vm.AddOp(vdbe.OpOpenEphemeral, currentCursor, numColumns, 0)

	// Store cursor numbers in temp tables
	tempTable.RootPage = uint32(resultCursor)
	currentTable.RootPage = uint32(currentCursor)

	// Register both temp tables in the schema
	s.conn.schema.Tables[tempTableName] = tempTable
	s.conn.schema.Tables[currentTableName] = currentTable

	// Step 1: Execute anchor member (non-recursive part)
	// The anchor is typically the left side of UNION
	anchorSelect := compound.Left

	// Rewrite anchor to use already-materialized CTEs
	rewrittenAnchor := s.rewriteSelectWithCTETables(anchorSelect, cteTempTables)

	// Execute anchor and collect all rows in memory first
	anchorVM := vdbe.New()
	anchorVM.Ctx = vm.Ctx
	compiledAnchor, err := s.compileSelect(anchorVM, rewrittenAnchor, args)
	if err != nil {
		return nil, fmt.Errorf("failed to compile recursive CTE anchor: %w", err)
	}

	// Collect all anchor rows
	var anchorRows [][]interface{}
	for {
		hasRow, err := compiledAnchor.Step()
		if err != nil {
			return nil, fmt.Errorf("anchor execution failed: %w", err)
		}
		if !hasRow {
			break
		}
		// Copy the result row
		row := make([]interface{}, numColumns)
		for i := 0; i < numColumns && i < len(compiledAnchor.ResultRow); i++ {
			row[i] = compiledAnchor.ResultRow[i].Value()
		}
		anchorRows = append(anchorRows, row)
	}

	// Materialize anchor results into both result and current tables
	baseReg := len(vm.Mem)
	vm.AllocMemory(baseReg + numColumns + 2)
	recordReg := baseReg + numColumns
	for _, row := range anchorRows {
		for i := 0; i < numColumns; i++ {
			// Emit bytecode to load value into register
			switch v := row[i].(type) {
			case nil:
				vm.AddOp(vdbe.OpNull, 0, baseReg+i, 0)
			case int64:
				vm.AddOp(vdbe.OpInteger, int(v), baseReg+i, 0)
			case float64:
				vm.AddOpWithP4Real(vdbe.OpReal, 0, baseReg+i, 0, v)
			case string:
				vm.AddOpWithP4Str(vdbe.OpString8, 0, baseReg+i, 0, v)
			case []byte:
				vm.AddOpWithP4Blob(vdbe.OpBlob, len(v), baseReg+i, 0, v)
			default:
				vm.AddOp(vdbe.OpNull, 0, baseReg+i, 0)
			}
		}
		vm.AddOp(vdbe.OpMakeRecord, baseReg, numColumns, recordReg)
		vm.AddOp(vdbe.OpInsert, resultCursor, recordReg, 0)
		vm.AddOp(vdbe.OpInsert, currentCursor, recordReg, 0)
	}

	// Step 2: Iterate recursive member until no new rows
	// The recursive member is typically the right side of UNION
	recursiveMember := compound.Right
	maxIterations := 1000

	for iteration := 0; iteration < maxIterations; iteration++ {
		// Compile recursive member with CTE reference pointing to current table
		recursiveTempTables := make(map[string]*schema.Table)
		for k, v := range cteTempTables {
			recursiveTempTables[k] = v
		}
		recursiveTempTables[cteName] = currentTable

		rewrittenRecursive := s.rewriteSelectWithCTETables(recursiveMember, recursiveTempTables)
		recursiveVM := vdbe.New()
		recursiveVM.Ctx = vm.Ctx
		compiledRecursive, err := s.compileSelect(recursiveVM, rewrittenRecursive, args)
		if err != nil {
			return nil, fmt.Errorf("failed to compile recursive member: %w", err)
		}

		// Execute recursive member and collect all new rows
		var newRows [][]interface{}
		for {
			hasRow, err := compiledRecursive.Step()
			if err != nil {
				return nil, fmt.Errorf("recursive member execution failed: %w", err)
			}
			if !hasRow {
				break
			}
			// Copy the result row
			row := make([]interface{}, numColumns)
			for i := 0; i < numColumns && i < len(compiledRecursive.ResultRow); i++ {
				row[i] = compiledRecursive.ResultRow[i].Value()
			}
			newRows = append(newRows, row)
		}

		// If no new rows, exit loop
		if len(newRows) == 0 {
			break
		}

		// Materialize new rows into both result and current tables
		for _, row := range newRows {
			for i := 0; i < numColumns; i++ {
				// Emit bytecode to load value into register
				switch v := row[i].(type) {
				case nil:
					vm.AddOp(vdbe.OpNull, 0, baseReg+i, 0)
				case int64:
					vm.AddOp(vdbe.OpInteger, int(v), baseReg+i, 0)
				case float64:
					vm.AddOpWithP4Real(vdbe.OpReal, 0, baseReg+i, 0, v)
				case string:
					vm.AddOpWithP4Str(vdbe.OpString8, 0, baseReg+i, 0, v)
				case []byte:
					vm.AddOpWithP4Blob(vdbe.OpBlob, len(v), baseReg+i, 0, v)
				default:
					vm.AddOp(vdbe.OpNull, 0, baseReg+i, 0)
				}
			}
			vm.AddOp(vdbe.OpMakeRecord, baseReg, numColumns, recordReg)
			vm.AddOp(vdbe.OpInsert, resultCursor, recordReg, 0)
			vm.AddOp(vdbe.OpInsert, currentCursor, recordReg, 0)
		}
	}

	return tempTable, nil
}


func (s *Stmt) createCTETempTable(tableName string, def *planner.CTEDefinition) *schema.Table {
	// Infer columns from CTE definition
	var columns []*schema.Column

	if len(def.Columns) > 0 {
		// Use explicit column list
		columns = make([]*schema.Column, len(def.Columns))
		for i, colName := range def.Columns {
			columns[i] = &schema.Column{
				Name:    colName,
				Type:    "ANY",
				NotNull: false,
			}
		}
	} else if def.Select != nil && len(def.Select.Columns) > 0 {
		// Infer from SELECT columns
		columns = make([]*schema.Column, len(def.Select.Columns))
		for i, col := range def.Select.Columns {
			colName := col.Alias
			if colName == "" {
				if ident, ok := col.Expr.(*parser.IdentExpr); ok {
					colName = ident.Name
				} else {
					colName = fmt.Sprintf("column_%d", i+1)
				}
			}
			columns[i] = &schema.Column{
				Name:    colName,
				Type:    "ANY",
				NotNull: false,
			}
		}
	}

	return &schema.Table{
		Name:     tableName,
		Columns:  columns,
		RootPage: 0, // Will be set to cursor number
		Temp:     true, // Mark as temporary/ephemeral table
	}
}

// rewriteSelectWithCTETables rewrites a SELECT statement to replace CTE references
// with their corresponding temporary tables.
func (s *Stmt) rewriteSelectWithCTETables(stmt *parser.SelectStmt, cteTempTables map[string]*schema.Table) *parser.SelectStmt {
	if stmt == nil {
		return nil
	}

	// Create a copy of the statement to avoid modifying the original
	rewritten := *stmt

	// Rewrite FROM clause
	if rewritten.From != nil {
		rewritten.From = s.rewriteFromClause(rewritten.From, cteTempTables)
	}

	// Recursively rewrite compound queries
	if rewritten.Compound != nil {
		compound := *rewritten.Compound
		compound.Left = s.rewriteSelectWithCTETables(compound.Left, cteTempTables)
		compound.Right = s.rewriteSelectWithCTETables(compound.Right, cteTempTables)
		rewritten.Compound = &compound
	}

	return &rewritten
}

// rewriteFromClause rewrites a FROM clause to replace CTE references.
func (s *Stmt) rewriteFromClause(from *parser.FromClause, cteTempTables map[string]*schema.Table) *parser.FromClause {
	if from == nil {
		return nil
	}

	rewritten := *from

	// Rewrite base tables
	rewrittenTables := make([]parser.TableOrSubquery, len(from.Tables))
	for i, table := range from.Tables {
		rewrittenTables[i] = s.rewriteTableOrSubquery(table, cteTempTables)
	}
	rewritten.Tables = rewrittenTables

	// Rewrite JOINs
	if len(from.Joins) > 0 {
		rewrittenJoins := make([]parser.JoinClause, len(from.Joins))
		for i, join := range from.Joins {
			rewrittenJoin := join
			rewrittenJoin.Table = s.rewriteTableOrSubquery(join.Table, cteTempTables)
			rewrittenJoins[i] = rewrittenJoin
		}
		rewritten.Joins = rewrittenJoins
	}

	return &rewritten
}

// rewriteTableOrSubquery rewrites a table reference, replacing CTE names with temp tables.
func (s *Stmt) rewriteTableOrSubquery(table parser.TableOrSubquery, cteTempTables map[string]*schema.Table) parser.TableOrSubquery {
	// Check if this table name references a CTE
	if tempTable, exists := cteTempTables[table.TableName]; exists {
		// Replace with temp table name
		rewritten := table
		rewritten.TableName = tempTable.Name
		return rewritten
	}

	// If it's a subquery, recursively rewrite it
	if table.Subquery != nil {
		rewritten := table
		rewritten.Subquery = s.rewriteSelectWithCTETables(table.Subquery, cteTempTables)
		return rewritten
	}

	return table
}
