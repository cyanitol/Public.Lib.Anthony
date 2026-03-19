// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

import (
	"database/sql/driver"

	"github.com/cyanitol/Public.Lib.Anthony/internal/parser"
	"github.com/cyanitol/Public.Lib.Anthony/internal/vdbe"
)

// compileCTEPopulationCoroutine generates bytecode to populate an ephemeral table using a coroutine.
// This allows the CTE to materialize at runtime instead of compile time.
//
// Bytecode pattern:
//
//	OpOpenEphemeral cursorNum, numColumns  // Open cursor OUTSIDE coroutine
//	OpInitCoroutine coroutineID, jumpPastBody, entryPoint
//	[coroutine body - executes SELECT and inserts into cursor]
//	OpEndCoroutine coroutineID
//	OpYield coroutineID  // Call the coroutine to materialize results
func (s *Stmt) compileCTEPopulationCoroutine(vm *vdbe.VDBE, cteSelect *parser.SelectStmt, cursorNum int, coroutineID int, numColumns int, args []driver.NamedValue) error {
	// CRITICAL: Open the ephemeral table BEFORE the coroutine, not inside it.
	// This ensures the cursor stays open after the coroutine completes.
	vm.AddOp(vdbe.OpOpenEphemeral, cursorNum, numColumns, 0)

	// Save the current address - this is where we'll place the InitCoroutine
	initAddr := vm.NumOps()

	// OpInitCoroutine: P1=coroutine ID, P2=jump past coroutine body (to be patched), P3=entry point
	vm.AddOp(vdbe.OpInitCoroutine, coroutineID, 0, 0)

	// Mark the coroutine entry point (where OpYield will jump to)
	coroutineEntry := vm.NumOps()

	// Compile the CTE SELECT to generate rows
	compiledCTE, err := s.compileCTESelect(vm, cteSelect, args)
	if err != nil {
		return err
	}

	// Allocate resources for inlining CTE bytecode
	offsets := s.allocateCTEResources(vm, compiledCTE)

	// Copy CTE bytecode into main VM with adjustments, using coroutine-aware handling
	s.inlineCTEBytecodeForCoroutine(vm, compiledCTE, cursorNum, coroutineID, offsets)

	// After the SELECT completes, end the coroutine (but DON'T close the cursor)
	vm.AddOp(vdbe.OpEndCoroutine, coroutineID, 0, 0)

	// Now we know where the coroutine body ends, patch the InitCoroutine instruction
	coroutineEnd := vm.NumOps()
	vm.Program[initAddr].P2 = coroutineEnd
	vm.Program[initAddr].P3 = coroutineEntry

	// Now emit the code that calls the coroutine to materialize results
	// OpYield: P1=coroutine ID, P2=return address register (0=use PC)
	vm.AddOp(vdbe.OpYield, coroutineID, 0, 0)

	return nil
}

// inlineCTEBytecodeForCoroutine copies CTE bytecode into main VM with necessary adjustments for coroutine execution.
// Uses a proper address map to handle ResultRow expansion (1 instruction -> 2: MakeRecord + Insert).
func (s *Stmt) inlineCTEBytecodeForCoroutine(vm *vdbe.VDBE, compiledCTE *vdbe.VDBE, cursorNum int, coroutineID int, offsets cteInlineOffsets) {
	addrMap := s.buildSingleInsertAddrMap(compiledCTE, offsets.startAddr)

	for _, instr := range compiledCTE.Program {
		newInstr := s.adjustInstructionParameters(instr, offsets)

		// Handle special opcodes for coroutine context
		if s.handleSpecialOpcodeForCoroutine(vm, instr, &newInstr, cursorNum, coroutineID, offsets) {
			continue // Instruction already added or skipped
		}

		// Add the instruction
		addr := vm.AddOp(newInstr.Opcode, newInstr.P1, newInstr.P2, newInstr.P3)
		vm.Program[addr].P4 = instr.P4
		vm.Program[addr].P4Type = instr.P4Type
		vm.Program[addr].P5 = instr.P5
		vm.Program[addr].Comment = instr.Comment

		// Adjust jump targets using address map
		s.fixJumpWithAddrMap(vm, instr, addr, addrMap)
	}
}

// handleSpecialOpcodeForCoroutine handles ResultRow and Halt opcodes specially in coroutine context.
// Returns true if handled.
func (s *Stmt) handleSpecialOpcodeForCoroutine(vm *vdbe.VDBE, instr *vdbe.Instruction, newInstr *vdbe.Instruction, cursorNum int, coroutineID int, offsets cteInlineOffsets) bool {
	switch instr.Opcode {
	case vdbe.OpResultRow:
		// Replace ResultRow with MakeRecord + Insert to populate the ephemeral table
		newInstr.Opcode = vdbe.OpMakeRecord
		newInstr.P3 = offsets.recordReg

		addr := vm.AddOp(newInstr.Opcode, newInstr.P1, newInstr.P2, newInstr.P3)
		vm.Program[addr].P4 = instr.P4
		vm.Program[addr].Comment = "CTE: Convert result row to record"

		vm.AddOp(vdbe.OpInsert, cursorNum, offsets.recordReg, 0)
		return true

	case vdbe.OpInit:
		// Replace Init with Noop - control flow is handled by the coroutine
		newInstr.Opcode = vdbe.OpNoop
		return false

	case vdbe.OpHalt:
		// Replace Halt with Noop - we'll handle termination with OpEndCoroutine
		newInstr.Opcode = vdbe.OpNoop
		return false
	}
	return false
}
