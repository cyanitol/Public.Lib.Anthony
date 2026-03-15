// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package vdbe

// Trigger execution VDBE opcodes and helpers.

// TriggerCompilerInterface defines the callback for compiling and executing
// trigger body statements at VDBE runtime.
type TriggerCompilerInterface interface {
	// ExecuteTriggers compiles and executes triggers for a given table and event.
	// tableName is the target table.
	// event is 0=INSERT, 1=UPDATE, 2=DELETE.
	// timing is 0=BEFORE, 1=AFTER.
	// triggerRow contains OLD/NEW pseudo-table data.
	// updatedCols is the list of updated columns (for UPDATE triggers only).
	ExecuteTriggers(tableName string, event int, timing int,
		triggerRow *TriggerRowData, updatedCols []string) error
}

// execTriggerBefore executes BEFORE triggers for a DML operation.
// P1 = event type (0=INSERT, 1=UPDATE, 2=DELETE)
// P2 = address to jump to if RAISE(IGNORE) is encountered
// P3 = register containing updated columns count (for UPDATE)
// P4.Z = table name
func (v *VDBE) execTriggerBefore(instr *Instruction) error {
	return v.executeTriggerOp(instr, triggerTimingBefore)
}

// execTriggerAfter executes AFTER triggers for a DML operation.
// P1 = event type (0=INSERT, 1=UPDATE, 2=DELETE)
// P2 = unused
// P3 = register containing updated columns count (for UPDATE)
// P4.Z = table name
func (v *VDBE) execTriggerAfter(instr *Instruction) error {
	return v.executeTriggerOp(instr, triggerTimingAfter)
}

// Trigger timing constants matching parser.TriggerTiming.
const (
	triggerTimingBefore = 0
	triggerTimingAfter  = 1
)

// executeTriggerOp is the shared implementation for trigger execution.
func (v *VDBE) executeTriggerOp(instr *Instruction, timing int) error {
	tableName := instr.P4.Z
	if tableName == "" {
		return nil // No table name, skip triggers
	}

	compiler := v.getTriggerCompiler()
	if compiler == nil {
		return nil // No trigger compiler available, skip
	}

	triggerRow := v.TriggerRow
	if triggerRow == nil {
		triggerRow = &TriggerRowData{}
	}

	err := compiler.ExecuteTriggers(tableName, instr.P1, timing, triggerRow, nil)
	if err != nil {
		return v.handleTriggerError(err, instr)
	}
	return nil
}

// getTriggerCompiler extracts the trigger compiler from the VDBE context.
func (v *VDBE) getTriggerCompiler() TriggerCompilerInterface {
	if v.Ctx == nil || v.Ctx.TriggerCompiler == nil {
		return nil
	}
	compiler, ok := v.Ctx.TriggerCompiler.(TriggerCompilerInterface)
	if !ok {
		return nil
	}
	return compiler
}

// handleTriggerError processes errors from trigger execution.
// For RAISE(IGNORE), it jumps to P2. For other errors, it propagates.
func (v *VDBE) handleTriggerError(err error, instr *Instruction) error {
	raiseErr, ok := err.(*RaiseError)
	if !ok {
		return err
	}

	if raiseErr.IsIgnore() && instr.P2 > 0 {
		v.PC = instr.P2
		return nil
	}

	return raiseErr
}

// execRaise executes the RAISE function within a trigger body.
// P1 = raise type (0=IGNORE, 1=ROLLBACK, 2=ABORT, 3=FAIL)
// P4.Z = error message (empty for IGNORE)
func (v *VDBE) execRaise(instr *Instruction) error {
	return &RaiseError{
		Type:    instr.P1,
		Message: instr.P4.Z,
	}
}

// buildTriggerRowFromInsert builds a TriggerRowData for INSERT operations.
// It extracts the NEW row values from the VDBE registers.
func (v *VDBE) buildTriggerRowFromInsert(tableName string, recordReg int, rowidReg int) *TriggerRowData {
	newRow := v.extractRowFromRegisters(tableName, recordReg)
	if newRow == nil {
		newRow = make(map[string]interface{})
	}

	// Add rowid to the NEW row if available
	if rowidReg > 0 {
		rowidMem, err := v.GetMem(rowidReg)
		if err == nil && rowidMem != nil {
			newRow["rowid"] = rowidMem.IntValue()
		}
	}

	return &TriggerRowData{NewRow: newRow}
}

// buildTriggerRowFromDelete builds a TriggerRowData for DELETE operations.
// It extracts the OLD row values from the current cursor position.
func (v *VDBE) buildTriggerRowFromDelete(tableName string, cursorIdx int) *TriggerRowData {
	oldRow := v.extractRowFromCursor(tableName, cursorIdx)
	return &TriggerRowData{OldRow: oldRow}
}

// buildTriggerRowFromUpdate builds a TriggerRowData for UPDATE operations.
// It extracts OLD from current cursor and NEW from registers.
func (v *VDBE) buildTriggerRowFromUpdate(tableName string, cursorIdx int, recordReg int) *TriggerRowData {
	oldRow := v.extractRowFromCursor(tableName, cursorIdx)
	newRow := v.extractRowFromRegisters(tableName, recordReg)
	return &TriggerRowData{OldRow: oldRow, NewRow: newRow}
}

// extractRowFromRegisters extracts column values from contiguous registers.
func (v *VDBE) extractRowFromRegisters(tableName string, startReg int) map[string]interface{} {
	colNames := v.getTableColumnNames(tableName)
	if len(colNames) == 0 {
		return nil
	}

	row := make(map[string]interface{}, len(colNames))
	for i, name := range colNames {
		mem, err := v.GetMem(startReg + i)
		if err != nil || mem == nil {
			row[name] = nil
			continue
		}
		row[name] = memToGoValue(mem)
	}
	return row
}

// extractRowFromCursor extracts column values from the current cursor row.
func (v *VDBE) extractRowFromCursor(tableName string, cursorIdx int) map[string]interface{} {
	colNames := v.getTableColumnNames(tableName)
	if len(colNames) == 0 {
		return nil
	}

	cursor, err := v.GetCursor(cursorIdx)
	if err != nil || cursor == nil {
		return nil
	}

	return v.readCursorColumns(cursorIdx, colNames)
}

// readCursorColumns reads all column values from a cursor for the given columns.
func (v *VDBE) readCursorColumns(cursorIdx int, colNames []string) map[string]interface{} {
	row := make(map[string]interface{}, len(colNames))
	for i, name := range colNames {
		tempReg := v.NumMem - 1 // Use last register as temp
		if tempReg < 0 {
			row[name] = nil
			continue
		}
		// Simulate OpColumn to read a column value
		mem, err := v.GetMem(tempReg)
		if err != nil || mem == nil {
			row[name] = nil
			continue
		}
		colInstr := &Instruction{Opcode: OpColumn, P1: cursorIdx, P2: i, P3: tempReg}
		if execErr := v.execColumnDirect(colInstr); execErr != nil {
			row[name] = nil
			continue
		}
		row[name] = memToGoValue(mem)
	}
	return row
}

// execColumnDirect executes an OpColumn instruction directly (for internal use).
func (v *VDBE) execColumnDirect(instr *Instruction) error {
	return v.execColumn(instr)
}

// getTableColumnNames returns column names for a table from the schema.
func (v *VDBE) getTableColumnNames(tableName string) []string {
	if v.Ctx == nil || v.Ctx.Schema == nil {
		return nil
	}

	type schemaWithTable interface {
		GetTableByName(name string) (interface{}, bool)
	}
	schemaObj, ok := v.Ctx.Schema.(schemaWithTable)
	if !ok {
		return nil
	}

	tableObj, found := schemaObj.GetTableByName(tableName)
	if !found || tableObj == nil {
		return nil
	}

	type tableWithColumns interface {
		GetColumnNames() []string
	}
	tbl, ok := tableObj.(tableWithColumns)
	if !ok {
		return nil
	}

	return tbl.GetColumnNames()
}

// memToGoValue converts a Mem cell to a Go interface{} value.
func memToGoValue(mem *Mem) interface{} {
	if mem == nil {
		return nil
	}
	flags := mem.GetFlags()
	switch {
	case flags&MemNull != 0:
		return nil
	case flags&MemInt != 0:
		return mem.IntValue()
	case flags&MemReal != 0:
		return mem.RealValue()
	case flags&MemStr != 0:
		return mem.StrValue()
	case flags&MemBlob != 0:
		return mem.BlobValue()
	default:
		return nil
	}
}

