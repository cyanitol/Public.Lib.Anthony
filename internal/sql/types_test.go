// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package sql

import (
	"testing"
)

// TestParseAllocReg tests allocating a single register
func TestParseAllocReg(t *testing.T) {
	p := &Parse{Mem: 5}
	reg := p.AllocReg()
	if reg != 6 {
		t.Errorf("AllocReg() = %d, want 6", reg)
	}
	if p.Mem != 6 {
		t.Errorf("Parse.Mem = %d, want 6", p.Mem)
	}
}

// TestParseAllocRegs tests allocating multiple consecutive registers
func TestParseAllocRegs(t *testing.T) {
	p := &Parse{Mem: 3}
	base := p.AllocRegs(5)
	if base != 4 {
		t.Errorf("AllocRegs(5) = %d, want 4", base)
	}
	if p.Mem != 8 {
		t.Errorf("Parse.Mem = %d, want 8", p.Mem)
	}
}

// TestParseAllocCursor tests allocating a cursor number
func TestParseAllocCursor(t *testing.T) {
	p := &Parse{Tabs: 2}
	cursor := p.AllocCursor()
	if cursor != 3 {
		t.Errorf("AllocCursor() = %d, want 3", cursor)
	}
	if p.Tabs != 3 {
		t.Errorf("Parse.Tabs = %d, want 3", p.Tabs)
	}
}

// TestParseGetVdbe tests getting VDBE
func TestParseGetVdbe(t *testing.T) {
	p := &Parse{DB: &Database{Name: "test"}}
	vdbe := p.GetVdbe()
	if vdbe == nil {
		t.Error("GetVdbe() returned nil")
	}
	if p.Vdbe != vdbe {
		t.Error("GetVdbe() did not set Parse.Vdbe")
	}
}

// TestParseGetVdbeExisting tests getting existing VDBE
func TestParseGetVdbeExisting(t *testing.T) {
	existingVdbe := &Vdbe{NumCols: 5}
	p := &Parse{Vdbe: existingVdbe}
	vdbe := p.GetVdbe()
	if vdbe != existingVdbe {
		t.Error("GetVdbe() returned different VDBE")
	}
}

// TestNewSrcList tests creating a new source list
func TestNewSrcList(t *testing.T) {
	sl := NewSrcList()
	if sl == nil {
		t.Fatal("NewSrcList() returned nil")
	}
	if sl.Len() != 0 {
		t.Errorf("NewSrcList().Len() = %d, want 0", sl.Len())
	}
}

// TestSrcListLen tests getting length of source list
func TestSrcListLen(t *testing.T) {
	tests := []struct {
		name     string
		list     *SrcList
		expected int
	}{
		{"nil list", nil, 0},
		{"empty list", &SrcList{Items: []SrcListItem{}}, 0},
		{"one item", &SrcList{Items: []SrcListItem{{Name: "test"}}}, 1},
		{"multiple items", &SrcList{Items: []SrcListItem{{Name: "a"}, {Name: "b"}}}, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.list.Len(); got != tt.expected {
				t.Errorf("Len() = %d, want %d", got, tt.expected)
			}
		})
	}
}

// TestSrcListGet tests getting item from source list
func TestSrcListGet(t *testing.T) {
	item1 := SrcListItem{Name: "table1"}
	item2 := SrcListItem{Name: "table2"}
	sl := &SrcList{Items: []SrcListItem{item1, item2}}

	tests := []struct {
		name     string
		idx      int
		expected *SrcListItem
	}{
		{"first item", 0, &item1},
		{"second item", 1, &item2},
		{"negative index", -1, nil},
		{"out of bounds", 5, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sl.Get(tt.idx)
			if tt.expected == nil {
				if got != nil {
					t.Errorf("Get(%d) = %v, want nil", tt.idx, got)
				}
			} else if got == nil {
				t.Errorf("Get(%d) = nil, want non-nil", tt.idx)
			} else if got.Name != tt.expected.Name {
				t.Errorf("Get(%d).Name = %s, want %s", tt.idx, got.Name, tt.expected.Name)
			}
		})
	}
}

// TestSrcListGetNil tests getting from nil list
func TestSrcListGetNil(t *testing.T) {
	var sl *SrcList
	if got := sl.Get(0); got != nil {
		t.Errorf("nil.Get(0) = %v, want nil", got)
	}
}

// TestSrcListAppend tests appending to source list
func TestSrcListAppend(t *testing.T) {
	sl := NewSrcList()
	item := SrcListItem{Name: "test"}
	sl.Append(item)

	if sl.Len() != 1 {
		t.Errorf("After Append, Len() = %d, want 1", sl.Len())
	}
	if sl.Get(0).Name != "test" {
		t.Errorf("After Append, Get(0).Name = %s, want test", sl.Get(0).Name)
	}
}

// TestSrcListAppendNil tests appending to nil list
func TestSrcListAppendNil(t *testing.T) {
	var sl *SrcList
	item := SrcListItem{Name: "test"}
	// Should not panic
	sl.Append(item)
}

// TestNewExprList tests creating a new expression list
func TestNewExprList(t *testing.T) {
	el := NewExprList()
	if el == nil {
		t.Fatal("NewExprList() returned nil")
	}
	if el.Len() != 0 {
		t.Errorf("NewExprList().Len() = %d, want 0", el.Len())
	}
}

// TestExprListLen tests getting length of expression list
func TestExprListLen(t *testing.T) {
	tests := []struct {
		name     string
		list     *ExprList
		expected int
	}{
		{"nil list", nil, 0},
		{"empty list", &ExprList{Items: []ExprListItem{}}, 0},
		{"one item", &ExprList{Items: []ExprListItem{{Name: "col1"}}}, 1},
		{"multiple items", &ExprList{Items: []ExprListItem{{Name: "col1"}, {Name: "col2"}}}, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.list.Len(); got != tt.expected {
				t.Errorf("Len() = %d, want %d", got, tt.expected)
			}
		})
	}
}

// TestExprListGet tests getting item from expression list
func TestExprListGet(t *testing.T) {
	item1 := ExprListItem{Name: "col1"}
	item2 := ExprListItem{Name: "col2"}
	el := &ExprList{Items: []ExprListItem{item1, item2}}

	tests := []struct {
		name     string
		idx      int
		expected *ExprListItem
	}{
		{"first item", 0, &item1},
		{"second item", 1, &item2},
		{"negative index", -1, nil},
		{"out of bounds", 5, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := el.Get(tt.idx)
			if tt.expected == nil {
				if got != nil {
					t.Errorf("Get(%d) = %v, want nil", tt.idx, got)
				}
			} else if got == nil {
				t.Errorf("Get(%d) = nil, want non-nil", tt.idx)
			} else if got.Name != tt.expected.Name {
				t.Errorf("Get(%d).Name = %s, want %s", tt.idx, got.Name, tt.expected.Name)
			}
		})
	}
}

// TestExprListGetNil tests getting from nil list
func TestExprListGetNil(t *testing.T) {
	var el *ExprList
	if got := el.Get(0); got != nil {
		t.Errorf("nil.Get(0) = %v, want nil", got)
	}
}

// TestExprListAppend tests appending to expression list
func TestExprListAppend(t *testing.T) {
	el := NewExprList()
	item := ExprListItem{Name: "test"}
	el.Append(item)

	if el.Len() != 1 {
		t.Errorf("After Append, Len() = %d, want 1", el.Len())
	}
	if el.Get(0).Name != "test" {
		t.Errorf("After Append, Get(0).Name = %s, want test", el.Get(0).Name)
	}
}

// TestExprListAppendNil tests appending to nil list
func TestExprListAppendNil(t *testing.T) {
	var el *ExprList
	item := ExprListItem{Name: "test"}
	// Should not panic
	el.Append(item)
}

// TestTableGetColumn tests getting column from table
func TestTableGetColumn(t *testing.T) {
	table := &Table{
		NumColumns: 2,
		Columns: []Column{
			{Name: "id"},
			{Name: "name"},
		},
	}

	tests := []struct {
		name     string
		idx      int
		expected *Column
	}{
		{"first column", 0, &table.Columns[0]},
		{"second column", 1, &table.Columns[1]},
		{"negative index", -1, nil},
		{"out of bounds", 5, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := table.GetColumn(tt.idx)
			if tt.expected == nil {
				if got != nil {
					t.Errorf("GetColumn(%d) = %v, want nil", tt.idx, got)
				}
			} else if got == nil {
				t.Errorf("GetColumn(%d) = nil, want non-nil", tt.idx)
			} else if got.Name != tt.expected.Name {
				t.Errorf("GetColumn(%d).Name = %s, want %s", tt.idx, got.Name, tt.expected.Name)
			}
		})
	}
}

// TestTableGetColumnNil tests getting column from nil table
func TestTableGetColumnNil(t *testing.T) {
	var table *Table
	if got := table.GetColumn(0); got != nil {
		t.Errorf("nil.GetColumn(0) = %v, want nil", got)
	}
}

// TestNewVdbe tests creating a new VDBE
func TestNewVdbe(t *testing.T) {
	db := &Database{Name: "test"}
	vdbe := NewVdbe(db)

	if vdbe == nil {
		t.Fatal("NewVdbe() returned nil")
	}
	if vdbe.DB != db {
		t.Error("NewVdbe() did not set DB correctly")
	}
	if len(vdbe.Ops) != 0 {
		t.Errorf("NewVdbe() Ops length = %d, want 0", len(vdbe.Ops))
	}
}

// TestVdbeAddOp1 tests adding 1-parameter instruction
func TestVdbeAddOp1(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	addr := vdbe.AddOp1(OP_Halt, 5)

	if addr != 0 {
		t.Errorf("AddOp1() returned addr %d, want 0", addr)
	}
	if len(vdbe.Ops) != 1 {
		t.Fatalf("AddOp1() didn't add op, len = %d", len(vdbe.Ops))
	}
	if vdbe.Ops[0].Opcode != OP_Halt {
		t.Errorf("Op opcode = %v, want OP_Halt", vdbe.Ops[0].Opcode)
	}
	if vdbe.Ops[0].P1 != 5 {
		t.Errorf("Op P1 = %d, want 5", vdbe.Ops[0].P1)
	}
}

// TestVdbeAddOp2 tests adding 2-parameter instruction
func TestVdbeAddOp2(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	addr := vdbe.AddOp2(OP_Integer, 10, 2)

	if addr != 0 {
		t.Errorf("AddOp2() returned addr %d, want 0", addr)
	}
	if vdbe.Ops[0].P1 != 10 {
		t.Errorf("Op P1 = %d, want 10", vdbe.Ops[0].P1)
	}
	if vdbe.Ops[0].P2 != 2 {
		t.Errorf("Op P2 = %d, want 2", vdbe.Ops[0].P2)
	}
}

// TestVdbeAddOp3 tests adding 3-parameter instruction
func TestVdbeAddOp3(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	addr := vdbe.AddOp3(OP_Column, 0, 1, 2)

	if addr != 0 {
		t.Errorf("AddOp3() returned addr %d, want 0", addr)
	}
	if vdbe.Ops[0].P1 != 0 {
		t.Errorf("Op P1 = %d, want 0", vdbe.Ops[0].P1)
	}
	if vdbe.Ops[0].P2 != 1 {
		t.Errorf("Op P2 = %d, want 1", vdbe.Ops[0].P2)
	}
	if vdbe.Ops[0].P3 != 2 {
		t.Errorf("Op P3 = %d, want 2", vdbe.Ops[0].P3)
	}
}

// TestVdbeAddOp4 tests adding 4-parameter instruction
func TestVdbeAddOp4(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	p4 := "test string"
	addr := vdbe.AddOp4(OP_String8, 0, 1, 0, p4)

	if addr != 0 {
		t.Errorf("AddOp4() returned addr %d, want 0", addr)
	}
	if vdbe.Ops[0].P4 != p4 {
		t.Errorf("Op P4 = %v, want %s", vdbe.Ops[0].P4, p4)
	}
}

// TestVdbeAddOp4Int tests adding instruction with integer P4
func TestVdbeAddOp4Int(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	addr := vdbe.AddOp4Int(OP_Compare, 0, 1, 2, 42)

	if addr != 0 {
		t.Errorf("AddOp4Int() returned addr %d, want 0", addr)
	}
	if vdbe.Ops[0].P4 != 42 {
		t.Errorf("Op P4 = %v, want 42", vdbe.Ops[0].P4)
	}
}

// TestVdbeMakeLabel tests making a label
func TestVdbeMakeLabel(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	label := vdbe.MakeLabel()

	if label >= 0 {
		t.Errorf("MakeLabel() = %d, want negative value", label)
	}
}

// TestVdbeCurrentAddr tests getting current address
func TestVdbeCurrentAddr(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	if addr := vdbe.CurrentAddr(); addr != 0 {
		t.Errorf("CurrentAddr() = %d, want 0", addr)
	}

	vdbe.AddOp1(OP_Halt, 0)
	if addr := vdbe.CurrentAddr(); addr != 1 {
		t.Errorf("After AddOp1, CurrentAddr() = %d, want 1", addr)
	}
}

// TestVdbeSetNumCols tests setting number of columns
func TestVdbeSetNumCols(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	vdbe.SetNumCols(3)

	if vdbe.NumCols != 3 {
		t.Errorf("SetNumCols(3) set NumCols = %d, want 3", vdbe.NumCols)
	}
	if len(vdbe.ColNames) != 3 {
		t.Errorf("SetNumCols(3) set ColNames len = %d, want 3", len(vdbe.ColNames))
	}
	if len(vdbe.ColTypes) != 3 {
		t.Errorf("SetNumCols(3) set ColTypes len = %d, want 3", len(vdbe.ColTypes))
	}
}

// TestVdbeSetColName tests setting column name
func TestVdbeSetColName(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	vdbe.SetNumCols(2)
	vdbe.SetColName(0, "id")
	vdbe.SetColName(1, "name")

	if vdbe.ColNames[0] != "id" {
		t.Errorf("ColNames[0] = %s, want id", vdbe.ColNames[0])
	}
	if vdbe.ColNames[1] != "name" {
		t.Errorf("ColNames[1] = %s, want name", vdbe.ColNames[1])
	}
}

// TestVdbeSetColNameOutOfBounds tests setting column name out of bounds
func TestVdbeSetColNameOutOfBounds(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	vdbe.SetNumCols(1)
	// Should not panic
	vdbe.SetColName(5, "test")
}

// TestVdbeSetColDeclType tests setting column declared type
func TestVdbeSetColDeclType(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	vdbe.SetNumCols(1)
	vdbe.SetColDeclType(0, "INTEGER")

	if vdbe.ColTypes[0] != "INTEGER" {
		t.Errorf("ColTypes[0] = %s, want INTEGER", vdbe.ColTypes[0])
	}
}

// TestVdbeSetColDeclTypeOutOfBounds tests setting col type out of bounds
func TestVdbeSetColDeclTypeOutOfBounds(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	vdbe.SetNumCols(1)
	// Should not panic
	vdbe.SetColDeclType(5, "INTEGER")
}

// TestVdbeChangeP2 tests changing P2 parameter
func TestVdbeChangeP2(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	addr := vdbe.AddOp2(OP_Integer, 0, 0)
	vdbe.ChangeP2(addr, 42)

	if vdbe.Ops[addr].P2 != 42 {
		t.Errorf("After ChangeP2, P2 = %d, want 42", vdbe.Ops[addr].P2)
	}
}

// TestVdbeChangeP2OutOfBounds tests changing P2 out of bounds
func TestVdbeChangeP2OutOfBounds(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	// Should not panic
	vdbe.ChangeP2(5, 42)
}

// TestVdbeChangeP5 tests changing P5 parameter
func TestVdbeChangeP5(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	vdbe.AddOp1(OP_Halt, 0)
	vdbe.ChangeP5(10)

	if vdbe.Ops[0].P5 != 10 {
		t.Errorf("After ChangeP5, P5 = %d, want 10", vdbe.Ops[0].P5)
	}
}

// TestVdbeChangeP5Empty tests changing P5 on empty program
func TestVdbeChangeP5Empty(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	// Should not panic
	vdbe.ChangeP5(10)
}

// TestVdbeGetOp tests getting an instruction
func TestVdbeGetOp(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	addr := vdbe.AddOp2(OP_Integer, 5, 1)
	op := vdbe.GetOp(addr)

	if op == nil {
		t.Fatal("GetOp() returned nil")
	}
	if op.Opcode != OP_Integer {
		t.Errorf("GetOp() opcode = %v, want OP_Integer", op.Opcode)
	}
}

// TestVdbeGetOpOutOfBounds tests getting instruction out of bounds
func TestVdbeGetOpOutOfBounds(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	op := vdbe.GetOp(5)
	if op != nil {
		t.Errorf("GetOp(5) = %v, want nil", op)
	}
}

// TestVdbeComment tests adding comment to last instruction
func TestVdbeComment(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	vdbe.AddOp1(OP_Halt, 0)
	comment := "Test comment"
	vdbe.Comment(comment)

	if vdbe.Ops[0].Comment != comment {
		t.Errorf("Comment = %s, want %s", vdbe.Ops[0].Comment, comment)
	}
}

// TestVdbeCommentEmpty tests adding comment to empty program
func TestVdbeCommentEmpty(t *testing.T) {
	vdbe := NewVdbe(&Database{})
	// Should not panic
	vdbe.Comment("test")
}

// TestParseReleaseReg tests releasing a register
func TestParseReleaseReg(t *testing.T) {
	p := &Parse{Mem: 10}
	// This is a no-op in the current implementation but should not panic
	p.ReleaseReg(5)
	// Verify state is unchanged
	if p.Mem != 10 {
		t.Errorf("ReleaseReg should not modify Mem, got %d, want 10", p.Mem)
	}
}

// TestParseReleaseRegs tests releasing multiple registers
func TestParseReleaseRegs(t *testing.T) {
	p := &Parse{Mem: 10}
	// This is a no-op in the current implementation but should not panic
	p.ReleaseRegs(3, 5)
	// Verify state is unchanged
	if p.Mem != 10 {
		t.Errorf("ReleaseRegs should not modify Mem, got %d, want 10", p.Mem)
	}
}
