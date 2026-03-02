// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package vdbe

import (
	"fmt"
	"os"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/observability"
)

// example_debugMode demonstrates using VDBE debug mode with instruction tracing.
func example_debugMode() {
	// Create a new VDBE instance
	v := New()

	// Enable debug tracing
	v.SetDebugMode(DebugTrace)

	// Build a simple program
	v.AddOp(OpInteger, 42, 0, 0)
	v.SetComment(0, "Load integer 42 into R0")

	v.AddOp(OpInteger, 99, 1, 0)
	v.SetComment(1, "Load integer 99 into R1")

	v.AddOp(OpAdd, 0, 1, 2)
	v.SetComment(2, "Add R0 + R1 -> R2")

	v.AddOp(OpHalt, 0, 0, 0)

	// Allocate registers
	v.AllocMemory(3)

	// Execute the program
	for {
		hasMore, err := v.Step()
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			break
		}
		if !hasMore {
			break
		}
	}

	// Print the instruction log
	fmt.Println("Instruction trace:")
	for _, entry := range v.GetInstructionLog() {
		fmt.Println(entry)
	}

	// Print final register values
	fmt.Printf("\nFinal result: R2 = %d\n", v.Mem[2].IntValue())

}

// example_debugWithLogger demonstrates using VDBE debug mode with observability logger.
func example_debugWithLogger() {
	// Create a new VDBE instance
	v := New()

	// Create an observability logger
	logger := observability.NewLogger(observability.DebugLevel, os.Stdout, observability.TextFormat)

	// Enable debug tracing with logger
	v.SetDebugMode(DebugTrace | DebugRegisters)
	v.SetDebugLogger(logger)
	v.SetDebugLogLevel(observability.DebugLevel)

	// Build a simple program
	v.AddOp(OpInteger, 100, 0, 0)
	v.AddOp(OpInteger, 200, 1, 0)
	v.AddOp(OpHalt, 0, 0, 0)

	v.AllocMemory(2)

	// Execute the program (debug output will go to logger)
	for {
		hasMore, err := v.Step()
		if err != nil {
			break
		}
		if !hasMore {
			break
		}
	}

	fmt.Println("Execution complete")
	// Note: Logger output will appear on stdout before this line
}

// example_registerInspection demonstrates register and cursor inspection.
func example_registerInspection() {
	v := New()

	// Allocate memory and cursors
	v.AllocMemory(3)
	v.AllocCursors(2)

	// Set some register values
	v.Mem[0].SetInt(42)
	v.Mem[1].SetStr("hello")
	v.Mem[2].SetReal(3.14159)

	// Open a cursor
	v.OpenCursor(0, CursorBTree, 1, true)

	// Watch a specific register
	v.WatchRegister(0)

	// Dump registers
	fmt.Println("Register dump:")
	fmt.Println(v.DumpRegisters())

	fmt.Println("\nCursor dump:")
	fmt.Println(v.DumpCursors())

	// Output shows register and cursor state
}

// example_breakpoints demonstrates using breakpoints for debugging.
func example_breakpoints() {
	v := New()
	v.SetDebugMode(DebugTrace)

	// Build a program
	v.AddOp(OpInteger, 1, 0, 0)
	v.AddOp(OpInteger, 2, 1, 0)
	v.AddOp(OpInteger, 3, 2, 0)
	v.AddOp(OpInteger, 4, 3, 0)
	v.AddOp(OpHalt, 0, 0, 0)

	v.AllocMemory(4)

	// Set a breakpoint at instruction 2
	v.AddBreakpoint(2)

	fmt.Println("Executing until breakpoint...")

	// Execute until breakpoint
	for {
		hasMore, err := v.Step()
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			break
		}
		if !hasMore {
			if v.PC == 2 {
				fmt.Printf("Hit breakpoint at PC=%d\n", v.PC)
			}
			break
		}
	}

	// Output:
	// Executing until breakpoint...
	// Hit breakpoint at PC=2
}

// example_fullStateDebug demonstrates comprehensive state debugging.
func example_fullStateDebug() {
	v := New()

	// Enable all debug features
	v.SetDebugMode(DebugAll)

	// Build a simple program
	v.AddOp(OpInteger, 42, 0, 0)
	v.AddOp(OpHalt, 0, 0, 0)

	v.AllocMemory(1)
	v.AllocCursors(1)

	// Execute one step
	v.Step()

	// Dump complete state
	fmt.Println(v.DumpState())

	// Output shows comprehensive VDBE state including registers, cursors, and recent instructions
}
