// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package sql

import (
	"errors"
	"fmt"
)

// InsertStmt represents a compiled INSERT statement
type InsertStmt struct {
	Table        string
	Columns      []string
	Values       [][]Value
	IsOrReplace  bool
	IsOrIgnore   bool
	IsOrAbort    bool
	IsOrFail     bool
	IsOrRollback bool
}

// OpCode represents a VDBE opcode
type OpCode int

const (
	OpInit OpCode = iota
	OpHalt
	OpOpenWrite
	OpOpenRead
	OpClose
	OpNewRowid
	OpInsert
	OpDelete
	OpRowData
	OpColumn
	OpRowid
	OpMakeRecord
	OpInteger
	OpString
	OpReal
	OpBlob
	OpNull
	OpCopy
	OpMove
	OpGoto
	OpIf
	OpIfNot
	OpEq
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpAdd
	OpSubtract
	OpMultiply
	OpDivide
	OpNotFound
	OpNotExists
	OpSeek
	OpRewind
	OpNext
	OpPrev
	OpIdxInsert
	OpIdxDelete
	OpIdxRowid
	OpIdxLT
	OpIdxGE
	OpIdxGT
	OpResultRow
	OpAddImm
	OpMustBeInt
	OpAffinity
	OpTypeCheck
	OpFinishSeek
	OpFkCheck
)

// Instruction represents a single VDBE instruction
type Instruction struct {
	OpCode  OpCode
	P1      int
	P2      int
	P3      int
	P4      interface{} // Can be string, int, or other data
	P5      int
	Comment string
}

// Program represents a compiled VDBE program
type Program struct {
	Instructions []Instruction
	NumRegisters int
	NumCursors   int
}

// String returns a string representation of an opcode
func (op OpCode) String() string {
	names := map[OpCode]string{
		OpInit:       "Init",
		OpHalt:       "Halt",
		OpOpenWrite:  "OpenWrite",
		OpOpenRead:   "OpenRead",
		OpClose:      "Close",
		OpNewRowid:   "NewRowid",
		OpInsert:     "Insert",
		OpDelete:     "Delete",
		OpRowData:    "RowData",
		OpColumn:     "Column",
		OpRowid:      "Rowid",
		OpMakeRecord: "MakeRecord",
		OpInteger:    "Integer",
		OpString:     "String",
		OpReal:       "Real",
		OpBlob:       "Blob",
		OpNull:       "Null",
		OpCopy:       "Copy",
		OpMove:       "Move",
		OpGoto:       "Goto",
		OpIf:         "If",
		OpIfNot:      "IfNot",
		OpEq:         "Eq",
		OpNe:         "Ne",
		OpLt:         "Lt",
		OpLe:         "Le",
		OpGt:         "Gt",
		OpGe:         "Ge",
		OpAdd:        "Add",
		OpSubtract:   "Subtract",
		OpMultiply:   "Multiply",
		OpDivide:     "Divide",
		OpNotFound:   "NotFound",
		OpNotExists:  "NotExists",
		OpSeek:       "Seek",
		OpRewind:     "Rewind",
		OpNext:       "Next",
		OpPrev:       "Prev",
		OpIdxInsert:  "IdxInsert",
		OpIdxDelete:  "IdxDelete",
		OpIdxRowid:   "IdxRowid",
		OpIdxLT:      "IdxLT",
		OpIdxGE:      "IdxGE",
		OpIdxGT:      "IdxGT",
		OpResultRow:  "ResultRow",
		OpAddImm:     "AddImm",
		OpMustBeInt:  "MustBeInt",
		OpAffinity:   "Affinity",
		OpTypeCheck:  "TypeCheck",
		OpFinishSeek: "FinishSeek",
		OpFkCheck:    "FkCheck",
	}
	if name, ok := names[op]; ok {
		return name
	}
	return fmt.Sprintf("Unknown(%d)", op)
}

// CompileInsert compiles an INSERT statement into VDBE bytecode
//
// Generated code structure:
//
//	OP_Init         0, end
//	OP_OpenWrite    0, table_root
//	OP_NewRowid     0, reg_rowid
//	OP_Integer      reg_col1, value1
//	OP_String       reg_col2, value2
//	...
//	OP_MakeRecord   reg_col1, num_cols, reg_record
//	OP_Insert       0, reg_record, reg_rowid
//	OP_Close        0
//
// end:
//
//	OP_Halt
func CompileInsert(stmt *InsertStmt, tableRoot int) (*Program, error) {
	if err := validateInsertStmt(stmt); err != nil {
		return nil, err
	}

	numCols := computeNumCols(stmt)
	prog := newProgram()
	cursorNum := 0

	prog.add(OpInit, 0, 0, 0, nil, 0, "Initialize program")
	prog.add(OpOpenWrite, cursorNum, tableRoot, 0, nil, 0,
		fmt.Sprintf("Open table %s for writing", stmt.Table))

	if err := compileInsertRows(prog, stmt, numCols, cursorNum); err != nil {
		return nil, err
	}

	prog.add(OpClose, cursorNum, 0, 0, nil, 0, fmt.Sprintf("Close table %s", stmt.Table))
	prog.Instructions[0].P2 = len(prog.Instructions)
	prog.add(OpHalt, 0, 0, 0, nil, 0, "End program")

	return prog, nil
}

func validateInsertStmt(stmt *InsertStmt) error {
	if stmt == nil {
		return errors.New("nil insert statement")
	}
	if len(stmt.Values) == 0 {
		return errors.New("no values to insert")
	}
	return nil
}

func computeNumCols(stmt *InsertStmt) int {
	if len(stmt.Columns) > 0 {
		return len(stmt.Columns)
	}
	if len(stmt.Values) > 0 {
		return len(stmt.Values[0])
	}
	return 0
}

func newProgram() *Program {
	return &Program{
		Instructions: make([]Instruction, 0),
		NumRegisters: 0,
		NumCursors:   1,
	}
}

func compileInsertRows(prog *Program, stmt *InsertStmt, numCols, cursorNum int) error {
	for rowIdx, row := range stmt.Values {
		if len(row) != numCols {
			return fmt.Errorf("row %d has %d values, expected %d", rowIdx, len(row), numCols)
		}
		if err := compileInsertRow(prog, row, rowIdx, numCols, cursorNum); err != nil {
			return err
		}
	}
	return nil
}

func compileInsertRow(prog *Program, row []Value, rowIdx, numCols, cursorNum int) error {
	regRowid := prog.allocReg()
	regCols := prog.allocRegs(numCols)
	regRecord := prog.allocReg()

	prog.add(OpNewRowid, cursorNum, regRowid, 0, nil, 0,
		fmt.Sprintf("Generate new rowid for row %d", rowIdx))

	for i, val := range row {
		if err := prog.addValueLoad(val, regCols+i); err != nil {
			return fmt.Errorf("row %d, column %d: %v", rowIdx, i, err)
		}
	}

	prog.add(OpMakeRecord, regCols, numCols, regRecord, nil, 0,
		fmt.Sprintf("Make record from %d columns", numCols))
	prog.add(OpInsert, cursorNum, regRecord, regRowid, nil, 0,
		fmt.Sprintf("Insert row %d", rowIdx))
	return nil
}

// add appends an instruction to the program
func (p *Program) add(op OpCode, p1, p2, p3 int, p4 interface{}, p5 int, comment string) {
	p.Instructions = append(p.Instructions, Instruction{
		OpCode:  op,
		P1:      p1,
		P2:      p2,
		P3:      p3,
		P4:      p4,
		P5:      p5,
		Comment: comment,
	})
}

// allocReg allocates a new register
func (p *Program) allocReg() int {
	reg := p.NumRegisters
	p.NumRegisters++
	return reg
}

// allocRegs allocates n consecutive registers
func (p *Program) allocRegs(n int) int {
	reg := p.NumRegisters
	p.NumRegisters += n
	return reg
}

// addValueLoad adds instructions to load a value into a register
func (p *Program) addValueLoad(val Value, reg int) error {
	switch val.Type {
	case TypeNull:
		p.add(OpNull, 0, reg, 0, nil, 0, "Load NULL")
		return nil

	case TypeInteger:
		p.add(OpInteger, int(val.Int), reg, 0, nil, 0,
			fmt.Sprintf("Load integer %d", val.Int))
		return nil

	case TypeFloat:
		p.add(OpReal, 0, reg, 0, val.Float, 0,
			fmt.Sprintf("Load float %f", val.Float))
		return nil

	case TypeText:
		p.add(OpString, 0, reg, 0, val.Text, 0,
			fmt.Sprintf("Load string '%s'", val.Text))
		return nil

	case TypeBlob:
		p.add(OpBlob, len(val.Blob), reg, 0, val.Blob, 0,
			fmt.Sprintf("Load blob (%d bytes)", len(val.Blob)))
		return nil

	default:
		return fmt.Errorf("unsupported value type: %v", val.Type)
	}
}

// Disassemble returns a human-readable representation of the program
func (p *Program) Disassemble() string {
	result := fmt.Sprintf("Program: %d instructions, %d registers, %d cursors\n",
		len(p.Instructions), p.NumRegisters, p.NumCursors)
	result += fmt.Sprintf("%-4s %-12s %-4s %-4s %-4s %-8s %-4s %s\n",
		"Addr", "Opcode", "P1", "P2", "P3", "P4", "P5", "Comment")
	result += "--------------------------------------------------------------------------------\n"

	for i, inst := range p.Instructions {
		p4str := ""
		if inst.P4 != nil {
			switch v := inst.P4.(type) {
			case string:
				if len(v) > 20 {
					p4str = fmt.Sprintf("'%.17s...'", v)
				} else {
					p4str = fmt.Sprintf("'%s'", v)
				}
			case float64:
				p4str = fmt.Sprintf("%.6f", v)
			case []byte:
				p4str = fmt.Sprintf("<blob:%d>", len(v))
			default:
				p4str = fmt.Sprintf("%v", v)
			}
		}

		result += fmt.Sprintf("%-4d %-12s %-4d %-4d %-4d %-8s %-4d %s\n",
			i, inst.OpCode.String(), inst.P1, inst.P2, inst.P3,
			p4str, inst.P5, inst.Comment)
	}

	return result
}

// CompileInsertWithAutoInc compiles an INSERT with auto-increment support
func CompileInsertWithAutoInc(stmt *InsertStmt, tableRoot int, hasAutoInc bool) (*Program, error) {
	prog, err := CompileInsert(stmt, tableRoot)
	if err != nil {
		return nil, err
	}

	if hasAutoInc {
		// Add auto-increment handling
		// This would involve reading/updating sqlite_sequence table
		// For now, we'll just use the basic NewRowid which handles this
		// in the actual VDBE implementation
	}

	return prog, nil
}

// ValidateInsert performs validation on an INSERT statement
func ValidateInsert(stmt *InsertStmt) error {
	if stmt == nil {
		return errors.New("nil insert statement")
	}

	if stmt.Table == "" {
		return errors.New("table name is required")
	}

	if len(stmt.Values) == 0 {
		return errors.New("no values to insert")
	}

	// Check that all rows have the same number of columns
	numCols := len(stmt.Columns)
	if numCols == 0 && len(stmt.Values) > 0 {
		numCols = len(stmt.Values[0])
	}

	for i, row := range stmt.Values {
		if len(row) != numCols {
			return fmt.Errorf("row %d has %d values, expected %d",
				i, len(row), numCols)
		}
	}

	return nil
}

// NewInsertStmt creates a new INSERT statement
func NewInsertStmt(table string, columns []string, values [][]Value) *InsertStmt {
	return &InsertStmt{
		Table:   table,
		Columns: columns,
		Values:  values,
	}
}
