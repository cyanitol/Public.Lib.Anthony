// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package vdbe

import (
	"testing"
)

// Helper functions for sorter tests
func setupSorter(t *testing.T, v *VDBE, keyInfo *SorterKeyInfo, cols int) {
	t.Helper()
	openInstr := &Instruction{
		Opcode: OpSorterOpen,
		P1:     0,
		P2:     cols,
		P4:     P4Union{P: keyInfo},
		P4Type: P4Dynamic,
	}
	err := v.execSorterOpen(openInstr)
	if err != nil {
		t.Fatalf("SorterOpen failed: %v", err)
	}
	if len(v.Sorters) == 0 || v.Sorters[0] == nil {
		t.Fatal("Sorter not created")
	}
}

func insertSorterRow(t *testing.T, vdbe *VDBE, row []interface{}) {
	t.Helper()
	for i, val := range row {
		switch typedVal := val.(type) {
		case int64:
			vdbe.Mem[i].SetInt(typedVal)
		case string:
			vdbe.Mem[i].SetStr(typedVal)
		}
	}
	insertInstr := &Instruction{
		Opcode: OpSorterInsert,
		P1:     0,
		P2:     0,
		P3:     len(row),
	}
	err := vdbe.execSorterInsert(insertInstr)
	if err != nil {
		t.Fatalf("SorterInsert failed: %v", err)
	}
}

func sortAndVerify(t *testing.T, v *VDBE) {
	t.Helper()
	sortInstr := &Instruction{Opcode: OpSorterSort, P1: 0, P2: 99}
	v.PC = 10
	err := v.execSorterSort(sortInstr)
	if err != nil {
		t.Fatalf("SorterSort failed: %v", err)
	}
	if v.PC == 99 {
		t.Error("Should not jump when sorter has rows")
	}
	if !v.Sorters[0].IsSorted() {
		t.Error("Sorter should be marked as sorted")
	}
	if v.Sorters[0].GetCurrent() != -1 {
		t.Errorf("Sorter current should be -1 after sort, got %d", v.Sorters[0].GetCurrent())
	}
}

func verifyNextRow(t *testing.T, v *VDBE, expected int64, iteration int) {
	t.Helper()
	nextInstr := &Instruction{Opcode: OpSorterNext, P1: 0, P2: 15}
	v.PC = 10
	err := v.execSorterNext(nextInstr)
	if err != nil {
		t.Fatalf("SorterNext iteration %d failed: %v", iteration, err)
	}
	if v.PC != 15 {
		t.Errorf("Expected PC=15 after SorterNext, got %d", v.PC)
	}
	row := v.Sorters[0].CurrentRow()
	if row == nil {
		t.Fatalf("CurrentRow returned nil at iteration %d", iteration)
	}
	if row[0].IntValue() != expected {
		t.Errorf("Row %d: expected value %d, got %d", iteration, expected, row[0].IntValue())
	}
}

func verifySorterDataExtracted(t *testing.T, v *VDBE) {
	t.Helper()
	v.Sorters[0].SetCurrent(0)
	err := v.execSorterData(&Instruction{Opcode: OpSorterData, P1: 0, P2: 10, P3: 2})
	if err != nil {
		t.Fatalf("SorterData failed: %v", err)
	}
	if v.Mem[10].IntValue() != 1 {
		t.Errorf("Expected r10=1, got %d", v.Mem[10].IntValue())
	}
	if v.Mem[11].StringValue() != "first" {
		t.Errorf("Expected r11='first', got '%s'", v.Mem[11].StringValue())
	}
}

func verifySorterClose(t *testing.T, v *VDBE) {
	t.Helper()
	if err := v.execSorterClose(&Instruction{Opcode: OpSorterClose, P1: 0}); err != nil {
		t.Fatalf("SorterClose failed: %v", err)
	}
	if v.Sorters[0] != nil {
		t.Error("Sorter should be nil after close")
	}
}

// TestSorterBasicWorkflow tests the basic sorter workflow
func TestSorterBasicWorkflow(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(20)
	keyInfo := &SorterKeyInfo{KeyCols: []int{0}, Desc: []bool{false}, Collations: []string{""}}
	setupSorter(t, v, keyInfo, 2)

	insertSorterRow(t, v, []interface{}{int64(3), "third"})
	insertSorterRow(t, v, []interface{}{int64(1), "first"})
	insertSorterRow(t, v, []interface{}{int64(2), "second"})

	if v.Sorters[0].NumRows() != 3 {
		t.Errorf("Expected 3 rows in sorter, got %d", v.Sorters[0].NumRows())
	}

	sortAndVerify(t, v)
	for i, expected := range []int64{1, 2, 3} {
		verifyNextRow(t, v, expected, i)
	}

	v.PC = 10
	if err := v.execSorterNext(&Instruction{Opcode: OpSorterNext, P1: 0, P2: 15}); err != nil {
		t.Fatalf("Final SorterNext failed: %v", err)
	}
	if v.PC == 15 {
		t.Error("Should not jump when no more rows")
	}

	verifySorterDataExtracted(t, v)
	verifySorterClose(t, v)
}

func TestSorterDescending(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)

	keyInfo := &SorterKeyInfo{
		KeyCols:    []int{0},
		Desc:       []bool{true},
		Collations: []string{""},
	}
	setupSorter(t, v, keyInfo, 1)

	for _, val := range []int64{1, 3, 2} {
		insertSorterRow(t, v, []interface{}{val})
	}

	v.execSorterSort(&Instruction{Opcode: OpSorterSort, P1: 0, P2: 0})

	for i, expected := range []int64{3, 2, 1} {
		v.PC = 0
		v.execSorterNext(&Instruction{Opcode: OpSorterNext, P1: 0, P2: 10})
		row := v.Sorters[0].CurrentRow()
		if row[0].IntValue() != expected {
			t.Errorf("Row %d: expected %d, got %d", i, expected, row[0].IntValue())
		}
	}
}

func TestSorterEmpty(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)

	// Open sorter but don't insert anything
	v.execSorterOpen(&Instruction{
		Opcode: OpSorterOpen,
		P1:     0,
		P2:     1,
		P4:     P4Union{P: &SorterKeyInfo{}},
		P4Type: P4Dynamic,
	})

	// Sort should jump if empty
	v.PC = 5
	v.execSorterSort(&Instruction{
		Opcode: OpSorterSort,
		P1:     0,
		P2:     99, // Jump address for empty
	})

	if v.PC != 99 {
		t.Errorf("Expected PC=99 for empty sorter, got %d", v.PC)
	}
}

func TestSorterErrors(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)

	// Try to insert into non-existent sorter
	err := v.execSorterInsert(&Instruction{
		Opcode: OpSorterInsert,
		P1:     99, // Invalid sorter
		P2:     0,
		P3:     1,
	})

	if err == nil {
		t.Error("Expected error for invalid sorter")
	}

	// Try to sort non-existent sorter
	err = v.execSorterSort(&Instruction{
		Opcode: OpSorterSort,
		P1:     99,
		P2:     0,
	})

	if err == nil {
		t.Error("Expected error for invalid sorter")
	}

	// Try to iterate non-existent sorter
	err = v.execSorterNext(&Instruction{
		Opcode: OpSorterNext,
		P1:     99,
		P2:     10,
	})

	if err == nil {
		t.Error("Expected error for invalid sorter")
	}

	// Try to get data from non-existent sorter
	err = v.execSorterData(&Instruction{
		Opcode: OpSorterData,
		P1:     99,
		P2:     0,
		P3:     1,
	})

	if err == nil {
		t.Error("Expected error for invalid sorter")
	}
}

func TestSorterDataNoCurrentRow(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)

	v.execSorterOpen(&Instruction{
		Opcode: OpSorterOpen,
		P1:     0,
		P2:     1,
	})

	// Try to get data when no current row
	err := v.execSorterData(&Instruction{
		Opcode: OpSorterData,
		P1:     0,
		P2:     0,
		P3:     1,
	})

	if err == nil {
		t.Error("Expected error when no current row")
	}
}

func TestSorterMultipleColumns(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(20)

	keyInfo := &SorterKeyInfo{
		KeyCols:    []int{0, 1},
		Desc:       []bool{false, true},
		Collations: []string{"", ""},
	}
	setupSorter(t, v, keyInfo, 3)

	insertSorterRow(t, v, []interface{}{int64(1), int64(3), "a"})
	insertSorterRow(t, v, []interface{}{int64(1), int64(1), "b"})
	insertSorterRow(t, v, []interface{}{int64(2), int64(2), "c"})

	v.execSorterSort(&Instruction{Opcode: OpSorterSort, P1: 0, P2: 0})

	for i, expected := range []string{"a", "b", "c"} {
		v.PC = 0
		v.execSorterNext(&Instruction{Opcode: OpSorterNext, P1: 0, P2: 10})
		row := v.Sorters[0].CurrentRow()
		if row[2].StringValue() != expected {
			t.Errorf("Row %d: expected col3='%s', got '%s'", i, expected, row[2].StringValue())
		}
	}
}

func TestSorterOpenWithoutKeyInfo(t *testing.T) {
	t.Parallel()
	v := NewTestVDBE(10)

	// Open without key info
	err := v.execSorterOpen(&Instruction{
		Opcode: OpSorterOpen,
		P1:     0,
		P2:     2,
		P4:     P4Union{P: nil},
		P4Type: P4Dynamic,
	})

	if err != nil {
		t.Fatalf("SorterOpen without key info failed: %v", err)
	}

	// Should still work, just with default sorting
	if v.Sorters[0] == nil {
		t.Error("Sorter should be created even without key info")
	}
}
