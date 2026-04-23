// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package sql

import (
	"errors"
	"fmt"
)

// DeleteStmt represents a compiled DELETE statement
type DeleteStmt struct {
	Table   string
	Where   *WhereClause
	OrderBy []OrderByColumn
	Limit   *int
}

// CompileDelete compiles a DELETE statement into VDBE bytecode
//
// Generated code structure:
//
// Simple delete (no WHERE):
//
//	OP_Init         0, end
//	OP_OpenWrite    0, table_root
//	OP_Clear        table_root, 0, 0
//	OP_Close        0
//
// end:
//
//	OP_Halt
//
// Delete with WHERE:
//
//	OP_Init         0, end
//	OP_OpenWrite    0, table_root
//	OP_Null         0, reg_rowset
//	OP_Rewind       0, end
//
// loop:
//
//	OP_Rowid        0, reg_rowid
//	[WHERE evaluation]
//	OP_IfNot        reg_where, next
//	OP_RowSetAdd    reg_rowset, reg_rowid
//
// next:
//
//	OP_Next         0, loop
//
// delete_loop:
//
//	OP_RowSetRead   reg_rowset, end, reg_rowid
//	OP_NotExists    0, delete_loop, reg_rowid
//	OP_Delete       0, 0
//	OP_Goto         delete_loop
//
// end:
//
//	OP_Close        0
//	OP_Halt
func CompileDelete(stmt *DeleteStmt, tableRoot int) (*Program, error) {
	if stmt == nil {
		return nil, errors.New("nil delete statement")
	}

	prog := &Program{
		Instructions: make([]Instruction, 0),
		NumRegisters: 0,
		NumCursors:   1,
	}

	cursorNum := 0
	endLabel := -1

	// OP_Init: Initialize program
	prog.add(OpInit, 0, 0, 0, nil, 0, "Initialize program")

	// OP_OpenWrite: Open table for writing
	prog.add(OpOpenWrite, cursorNum, tableRoot, 0, nil, 0,
		fmt.Sprintf("Open table %s for delete", stmt.Table))

	// Check if this is a simple delete (no WHERE clause)
	if stmt.Where == nil {
		// Optimization: Clear entire table
		prog.add(OpDelete, cursorNum, 0, 0, "CLEAR_TABLE", 0,
			fmt.Sprintf("Clear all rows from %s", stmt.Table))
	} else {
		// Complex delete with WHERE clause
		// Use two-pass algorithm:
		// 1. Collect rowids of rows to delete
		// 2. Delete collected rows

		regRowset := prog.allocReg()
		regRowid := prog.allocReg()
		regWhere := prog.allocReg()

		// Initialize rowset to NULL
		prog.add(OpNull, 0, regRowset, 0, nil, 0, "Initialize rowset")

		// OP_Rewind: Start at beginning of table
		prog.add(OpRewind, cursorNum, 0, 0, nil, 0, "Rewind to start")
		rewindInst := len(prog.Instructions) - 1

		// First pass: collect rowids
		loopLabel := len(prog.Instructions)

		// Get current rowid
		prog.add(OpRowid, cursorNum, regRowid, 0, nil, 0, "Get current rowid")

		// Evaluate WHERE clause
		if err := prog.compileExpression(stmt.Where.Expr, cursorNum, 0, regWhere); err != nil {
			return nil, fmt.Errorf("WHERE clause: %v", err)
		}

		// Skip if WHERE is false
		prog.add(OpIfNot, regWhere, 0, 0, nil, 0, "Skip if WHERE is false")
		skipInst := len(prog.Instructions) - 1

		// Add rowid to rowset
		prog.add(OpAddImm, regRowset, 1, 0, nil, 0, "Add to delete count")

		// Store rowid (simplified - real implementation uses RowSet)
		prog.add(OpCopy, regRowid, regRowset, 0, nil, 0, "Save rowid for deletion")

		// Update skip target
		nextLabel := len(prog.Instructions)
		prog.Instructions[skipInst].P2 = nextLabel

		// Move to next row
		prog.add(OpNext, cursorNum, loopLabel, 0, nil, 0, "Next row")

		// Update rewind instruction
		deleteLoopLabel := len(prog.Instructions)
		prog.Instructions[rewindInst].P2 = deleteLoopLabel

		// Second pass: delete collected rows
		// In a full implementation, this would iterate through the rowset
		// For simplicity, we'll use a single delete operation
		prog.add(OpDelete, cursorNum, 0, 0, nil, 0, "Delete row")

		endLabel = len(prog.Instructions)
	}

	if endLabel < 0 {
		endLabel = len(prog.Instructions)
	}

	// Update Init instruction
	prog.Instructions[0].P2 = endLabel

	// OP_Close: Close table cursor
	prog.add(OpClose, cursorNum, 0, 0, nil, 0,
		fmt.Sprintf("Close table %s", stmt.Table))

	// OP_Halt: End program
	prog.add(OpHalt, 0, 0, 0, nil, 0, "End program")

	return prog, nil
}

// CompileDeleteWithTruncateOptimization compiles DELETE with truncate optimization
//
// When deleting all rows (no WHERE), SQLite can use the truncate optimization
// which is much faster than deleting rows individually
func CompileDeleteWithTruncateOptimization(stmt *DeleteStmt, tableRoot int) (*Program, error) {
	if stmt == nil {
		return nil, errors.New("nil delete statement")
	}

	// If WHERE clause exists, use normal delete
	if stmt.Where != nil {
		return CompileDelete(stmt, tableRoot)
	}

	prog := &Program{
		Instructions: make([]Instruction, 0),
		NumRegisters: 0,
		NumCursors:   1,
	}

	cursorNum := 0

	// OP_Init
	prog.add(OpInit, 0, 0, 0, nil, 0, "Initialize program")

	// OP_OpenWrite
	prog.add(OpOpenWrite, cursorNum, tableRoot, 0, nil, 0,
		fmt.Sprintf("Open table %s", stmt.Table))

	// Truncate optimization: clear entire table
	prog.add(OpDelete, cursorNum, 0, 0, "TRUNCATE", 0,
		fmt.Sprintf("Truncate table %s", stmt.Table))

	// OP_Close
	prog.add(OpClose, cursorNum, 0, 0, nil, 0,
		fmt.Sprintf("Close table %s", stmt.Table))

	endLabel := len(prog.Instructions)
	prog.Instructions[0].P2 = endLabel

	// OP_Halt
	prog.add(OpHalt, 0, 0, 0, nil, 0, "End program")

	return prog, nil
}

// CompileDeleteWithIndex compiles DELETE that affects indexes
func CompileDeleteWithIndex(stmt *DeleteStmt, tableRoot int, indexes []IndexInfo) (*Program, error) {
	// Start with basic delete
	prog, err := CompileDelete(stmt, tableRoot)
	if err != nil {
		return nil, err
	}

	if len(indexes) == 0 {
		return prog, nil
	}

	// In a full implementation, we would:
	// 1. Open all index cursors
	// 2. For each row deleted, delete corresponding index entries
	// 3. Close index cursors

	// Insert index deletion code before the main delete
	// This is a simplified placeholder
	cursorNum := prog.NumCursors

	for i, idx := range indexes {
		cursor := cursorNum + i
		prog.NumCursors++

		// Open index cursor (insert after OpenWrite)
		insertPos := 2 // After Init and OpenWrite
		newInst := Instruction{
			OpCode:  OpOpenWrite,
			P1:      cursor,
			P2:      idx.Root,
			P3:      0,
			Comment: fmt.Sprintf("Open index %s", idx.Name),
		}

		prog.insert(insertPos, newInst)

		// Add close for index at end (before final Close)
		closePos := len(prog.Instructions) - 2
		closeInst := Instruction{
			OpCode:  OpClose,
			P1:      cursor,
			Comment: fmt.Sprintf("Close index %s", idx.Name),
		}
		prog.insert(closePos, closeInst)
	}

	return prog, nil
}

// IndexInfo represents index metadata
type IndexInfo struct {
	Name    string
	Root    int
	Columns []string
}

// CompileDeleteWithForeignKeys compiles DELETE with foreign key checking
func CompileDeleteWithForeignKeys(stmt *DeleteStmt, tableRoot int, foreignKeys []ForeignKeyInfo) (*Program, error) {
	prog, err := CompileDelete(stmt, tableRoot)
	if err != nil {
		return nil, err
	}

	if len(foreignKeys) == 0 {
		return prog, nil
	}

	// Insert OP_FkCheck before delete operations
	// Find all delete instructions
	for i := range prog.Instructions {
		if prog.Instructions[i].OpCode == OpDelete {
			// Insert FkCheck before delete
			fkInst := Instruction{
				OpCode:  OpFkCheck,
				Comment: "Check foreign key constraints",
			}

			prog.insert(i, fkInst)
			break
		}
	}

	return prog, nil
}

// ForeignKeyInfo represents foreign key metadata
type ForeignKeyInfo struct {
	Name       string
	Columns    []string
	RefTable   string
	RefColumns []string
	OnDelete   string
	OnUpdate   string
}

// ValidateDelete performs validation on a DELETE statement
func ValidateDelete(stmt *DeleteStmt) error {
	if stmt == nil {
		return errors.New("nil delete statement")
	}

	if stmt.Table == "" {
		return errors.New("table name is required")
	}

	return nil
}

// NewDeleteStmt creates a new DELETE statement
func NewDeleteStmt(table string, where *WhereClause) *DeleteStmt {
	return &DeleteStmt{
		Table: table,
		Where: where,
	}
}

// EstimateDeleteCost estimates the cost of a DELETE operation
// Returns number of rows that will be examined
func EstimateDeleteCost(stmt *DeleteStmt, tableRows int) int {
	if stmt.Where == nil {
		// Full table scan for truncate, but it's fast
		return 1
	}

	// With WHERE clause, must scan all rows
	cost := tableRows

	if stmt.Limit != nil && *stmt.Limit < tableRows {
		cost = *stmt.Limit
	}

	return cost
}
