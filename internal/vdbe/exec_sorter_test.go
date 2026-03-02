// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package vdbe

import (
	"testing"
)

// TestSorterOperations tests all sorter-related opcodes
func TestSorterOperations(t *testing.T) {
	t.Parallel()
	t.Run("BasicSorterWorkflow", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(20)

		// Test SorterOpen
		keyInfo := &SorterKeyInfo{
			KeyCols:    []int{0},        // Sort by first column
			Desc:       []bool{false},   // Ascending order
			Collations: []string{""},    // Default collation
		}

		openInstr := &Instruction{
			Opcode: OpSorterOpen,
			P1:     0,              // Sorter cursor 0
			P2:     2,              // 2 columns
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

		// Insert some rows
		testData := []struct {
			col1 int64
			col2 string
		}{
			{3, "third"},
			{1, "first"},
			{2, "second"},
		}

		for _, row := range testData {
			v.Mem[0].SetInt(row.col1)
			v.Mem[1].SetStr(row.col2)

			insertInstr := &Instruction{
				Opcode: OpSorterInsert,
				P1:     0,  // Sorter cursor 0
				P2:     0,  // Start register
				P3:     2,  // 2 columns
			}

			err := v.execSorterInsert(insertInstr)
			if err != nil {
				t.Fatalf("SorterInsert failed: %v", err)
			}
		}

		if len(v.Sorters[0].Rows) != 3 {
			t.Errorf("Expected 3 rows in sorter, got %d", len(v.Sorters[0].Rows))
		}

		// Sort the rows
		sortInstr := &Instruction{
			Opcode: OpSorterSort,
			P1:     0,   // Sorter cursor 0
			P2:     99,  // Jump to 99 if empty (should not jump)
		}

		v.PC = 10
		err = v.execSorterSort(sortInstr)
		if err != nil {
			t.Fatalf("SorterSort failed: %v", err)
		}

		if v.PC == 99 {
			t.Error("Should not jump when sorter has rows")
		}

		if !v.Sorters[0].Sorted {
			t.Error("Sorter should be marked as sorted")
		}

		if v.Sorters[0].Current != -1 {
			t.Errorf("Sorter current should be -1 after sort, got %d", v.Sorters[0].Current)
		}

		// Iterate through sorted results
		nextInstr := &Instruction{
			Opcode: OpSorterNext,
			P1:     0,   // Sorter cursor 0
			P2:     15,  // Jump to 15 if more rows available
		}

		expectedOrder := []int64{1, 2, 3}
		for i, expected := range expectedOrder {
			v.PC = 10
			err = v.execSorterNext(nextInstr)
			if err != nil {
				t.Fatalf("SorterNext iteration %d failed: %v", i, err)
			}

			if v.PC != 15 {
				t.Errorf("Expected PC=15 after SorterNext, got %d", v.PC)
			}

			row := v.Sorters[0].CurrentRow()
			if row == nil {
				t.Fatalf("CurrentRow returned nil at iteration %d", i)
			}

			if row[0].IntValue() != expected {
				t.Errorf("Row %d: expected value %d, got %d", i, expected, row[0].IntValue())
			}
		}

		// Next call should not jump (no more rows)
		v.PC = 10
		err = v.execSorterNext(nextInstr)
		if err != nil {
			t.Fatalf("Final SorterNext failed: %v", err)
		}

		if v.PC == 15 {
			t.Error("Should not jump when no more rows")
		}

		// Test SorterData
		v.Sorters[0].Current = 0 // Reset to first row
		dataInstr := &Instruction{
			Opcode: OpSorterData,
			P1:     0,   // Sorter cursor 0
			P2:     10,  // Destination register 10
			P3:     2,   // Copy 2 columns
		}

		err = v.execSorterData(dataInstr)
		if err != nil {
			t.Fatalf("SorterData failed: %v", err)
		}

		if v.Mem[10].IntValue() != 1 {
			t.Errorf("Expected r10=1, got %d", v.Mem[10].IntValue())
		}

		if v.Mem[11].StringValue() != "first" {
			t.Errorf("Expected r11='first', got '%s'", v.Mem[11].StringValue())
		}

		// Test SorterClose
		closeInstr := &Instruction{
			Opcode: OpSorterClose,
			P1:     0,  // Sorter cursor 0
		}

		err = v.execSorterClose(closeInstr)
		if err != nil {
			t.Fatalf("SorterClose failed: %v", err)
		}

		if v.Sorters[0] != nil {
			t.Error("Sorter should be nil after close")
		}
	})

	t.Run("SorterDescending", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)

		// Sort in descending order
		keyInfo := &SorterKeyInfo{
			KeyCols:    []int{0},
			Desc:       []bool{true},  // Descending
			Collations: []string{""},
		}

		openInstr := &Instruction{
			Opcode: OpSorterOpen,
			P1:     0,
			P2:     1,
			P4:     P4Union{P: keyInfo},
			P4Type: P4Dynamic,
		}

		v.execSorterOpen(openInstr)

		// Insert values
		for _, val := range []int64{1, 3, 2} {
			v.Mem[0].SetInt(val)
			v.execSorterInsert(&Instruction{
				Opcode: OpSorterInsert,
				P1:     0,
				P2:     0,
				P3:     1,
			})
		}

		// Sort and iterate
		v.execSorterSort(&Instruction{Opcode: OpSorterSort, P1: 0, P2: 0})

		expectedOrder := []int64{3, 2, 1}
		for i, expected := range expectedOrder {
			v.PC = 0
			v.execSorterNext(&Instruction{Opcode: OpSorterNext, P1: 0, P2: 10})

			row := v.Sorters[0].CurrentRow()
			if row[0].IntValue() != expected {
				t.Errorf("Row %d: expected %d, got %d", i, expected, row[0].IntValue())
			}
		}
	})

	t.Run("SorterEmpty", func(t *testing.T) {
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
			P2:     99,  // Jump address for empty
		})

		if v.PC != 99 {
			t.Errorf("Expected PC=99 for empty sorter, got %d", v.PC)
		}
	})

	t.Run("SorterErrors", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(10)

		// Try to insert into non-existent sorter
		err := v.execSorterInsert(&Instruction{
			Opcode: OpSorterInsert,
			P1:     99,  // Invalid sorter
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
	})

	t.Run("SorterDataNoCurrentRow", func(t *testing.T) {
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
	})

	t.Run("SorterMultipleColumns", func(t *testing.T) {
		t.Parallel()
		v := NewTestVDBE(20)

		// Sort by two columns
		keyInfo := &SorterKeyInfo{
			KeyCols:    []int{0, 1},           // Sort by first two columns
			Desc:       []bool{false, true},   // First ASC, second DESC
			Collations: []string{"", ""},
		}

		v.execSorterOpen(&Instruction{
			Opcode: OpSorterOpen,
			P1:     0,
			P2:     3,
			P4:     P4Union{P: keyInfo},
			P4Type: P4Dynamic,
		})

		// Insert rows: (1, 3, 'a'), (1, 1, 'b'), (2, 2, 'c')
		testRows := []struct {
			col1, col2 int64
			col3       string
		}{
			{1, 3, "a"},
			{1, 1, "b"},
			{2, 2, "c"},
		}

		for _, row := range testRows {
			v.Mem[0].SetInt(row.col1)
			v.Mem[1].SetInt(row.col2)
			v.Mem[2].SetStr(row.col3)
			v.execSorterInsert(&Instruction{Opcode: OpSorterInsert, P1: 0, P2: 0, P3: 3})
		}

		v.execSorterSort(&Instruction{Opcode: OpSorterSort, P1: 0, P2: 0})

		// Expected order: (1,3,'a'), (1,1,'b'), (2,2,'c')
		// When col1 is same, sort by col2 DESC: 3 before 1
		expectedCol3 := []string{"a", "b", "c"}
		for i := 0; i < 3; i++ {
			v.PC = 0
			v.execSorterNext(&Instruction{Opcode: OpSorterNext, P1: 0, P2: 10})
			row := v.Sorters[0].CurrentRow()
			if row[2].StringValue() != expectedCol3[i] {
				t.Errorf("Row %d: expected col3='%s', got '%s'", i, expectedCol3[i], row[2].StringValue())
			}
		}
	})

	t.Run("SorterOpenWithoutKeyInfo", func(t *testing.T) {
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
	})
}
