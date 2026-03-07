// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package vdbe

import (
	"fmt"
)

// ExampleQueryStatistics demonstrates how to use query statistics.
func ExampleQueryStatistics() {
	vdbe := New()

	// Statistics are enabled by default
	// Create a simple program that loads and returns values
	vdbe.AllocMemory(3)
	vdbe.AddOp(OpInteger, 0, 1, 100) // Load 100 into r1
	vdbe.AddOp(OpInteger, 0, 2, 200) // Load 200 into r2
	vdbe.AddOp(OpResultRow, 1, 2, 0) // Return r1, r2
	vdbe.AddOp(OpHalt, 0, 0, 0)      // Halt

	// Run the program
	_ = vdbe.Run()

	// Get statistics
	stats := vdbe.GetStatistics()

	fmt.Printf("Instructions executed: %d\n", stats.NumInstructions)
	fmt.Printf("Rows returned: %d\n", stats.RowsRead)
	fmt.Printf("Memory cells allocated: %d\n", stats.AllocatedCells)

	// Output:
	// Instructions executed: 4
	// Rows returned: 1
	// Memory cells allocated: 3
}

// ExampleQueryStatistics_String demonstrates the String() method.
func ExampleQueryStatistics_String() {
	stats := NewQueryStatistics()
	stats.NumInstructions = 50
	stats.RowsRead = 10
	stats.RowsWritten = 0
	stats.ExecutionMS = 5
	stats.QueryType = "SELECT"
	stats.IsReadOnly = true

	// Print formatted statistics
	fmt.Println(stats.String())
	// Output will vary based on actual statistics
}

// ExampleVDBE_GetStatistics demonstrates retrieving statistics after execution.
func ExampleVDBE_GetStatistics() {
	vdbe := New()

	// Simple program with arithmetic and comparisons
	vdbe.AllocMemory(4)
	vdbe.AddOp(OpInteger, 0, 1, 10)  // r1 = 10
	vdbe.AddOp(OpInteger, 0, 2, 20)  // r2 = 20
	vdbe.AddOp(OpAdd, 1, 2, 3)       // r3 = r1 + r2
	vdbe.AddOp(OpGt, 3, 0, 1)        // r3 > r1? (comparison)
	vdbe.AddOp(OpResultRow, 3, 1, 0) // return r3
	vdbe.AddOp(OpHalt, 0, 0, 0)

	_ = vdbe.Run()

	stats := vdbe.GetStatistics()
	fmt.Printf("Comparisons: %d\n", stats.NumComparisons)
	fmt.Printf("Instructions: %d\n", stats.NumInstructions)

	// Output:
	// Comparisons: 1
	// Instructions: 6
}

// ExampleVDBE_DisableStatistics shows how to disable statistics.
func ExampleVDBE_DisableStatistics() {
	vdbe := New()

	// Disable statistics for performance-critical code
	vdbe.DisableStatistics()

	// Add and run program (no statistics collected)
	vdbe.AllocMemory(2)
	vdbe.AddOp(OpInteger, 0, 1, 42)
	vdbe.AddOp(OpResultRow, 1, 1, 0)
	vdbe.AddOp(OpHalt, 0, 0, 0)

	_ = vdbe.Run()

	// Statistics will be nil or empty
	stats := vdbe.GetStatistics()
	fmt.Printf("Instructions: %d\n", stats.NumInstructions)

	// Output:
	// Instructions: 0
}
