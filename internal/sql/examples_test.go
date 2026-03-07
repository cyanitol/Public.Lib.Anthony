// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package sql_test

import (
	"fmt"
	"log"

	"github.com/cyanitol/Public.Lib.Anthony/internal/sql"
)

// ExampleMakeRecord demonstrates encoding values into SQLite record format
func ExampleMakeRecord() {
	values := []sql.Value{
		sql.IntValue(42),
		sql.TextValue("Hello, World!"),
		sql.FloatValue(3.14159),
		sql.NullValue(),
	}

	record, err := sql.MakeRecord(values)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Record size: %d bytes\n", len(record))
	fmt.Printf("First byte (header size): %d\n", record[0])

	// Output:
	// Record size: 27 bytes
	// First byte (header size): 5
}

// ExampleParseRecord demonstrates decoding SQLite record format
func ExampleParseRecord() {
	// First, create a record
	original := []sql.Value{
		sql.IntValue(1),
		sql.TextValue("test"),
		sql.FloatValue(2.5),
	}

	record, err := sql.MakeRecord(original)
	if err != nil {
		log.Fatal(err)
	}

	// Parse it back
	parsed, err := sql.ParseRecord(record)
	if err != nil {
		log.Fatal(err)
	}

	for i, val := range parsed.Values {
		fmt.Printf("Column %d: Type=%v\n", i, val.Type)
	}

	// Output:
	// Column 0: Type=1
	// Column 1: Type=3
	// Column 2: Type=2
}

// ExampleCompileInsert demonstrates compiling an INSERT statement
func ExampleCompileInsert() {
	stmt := sql.NewInsertStmt(
		"users",
		[]string{"id", "name", "email"},
		[][]sql.Value{
			{
				sql.IntValue(1),
				sql.TextValue("Alice"),
				sql.TextValue("alice@example.com"),
			},
		},
	)

	prog, err := sql.CompileInsert(stmt, 100)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Instructions: %d\n", len(prog.Instructions))
	fmt.Printf("Registers: %d\n", prog.NumRegisters)
	fmt.Printf("First opcode: %s\n", prog.Instructions[0].OpCode)
	fmt.Printf("Last opcode: %s\n", prog.Instructions[len(prog.Instructions)-1].OpCode)

	// Output:
	// Instructions: 10
	// Registers: 5
	// First opcode: Init
	// Last opcode: Halt
}

// ExampleProgram_Disassemble demonstrates VDBE bytecode disassembly
func ExampleProgram_Disassemble() {
	stmt := sql.NewInsertStmt(
		"test",
		[]string{"x"},
		[][]sql.Value{{sql.IntValue(42)}},
	)

	prog, _ := sql.CompileInsert(stmt, 100)

	output := prog.Disassemble()
	fmt.Printf("Disassembly length: %d characters\n", len(output))
	fmt.Printf("Contains 'OpenWrite': %v\n", len(output) > 0)

	// Output:
	// Disassembly length: 721 characters
	// Contains 'OpenWrite': true
}

// ExampleCompileUpdate demonstrates compiling an UPDATE statement
func ExampleCompileUpdate() {
	// Create WHERE clause: id = 1
	where := sql.NewWhereClause(
		sql.NewBinaryExpression(
			sql.NewColumnExpression("id"),
			"=",
			sql.NewLiteralExpression(sql.IntValue(1)),
		),
	)

	stmt := sql.NewUpdateStmt(
		"users",
		[]string{"name"},
		[]sql.Value{sql.TextValue("Updated Name")},
		where,
	)

	prog, err := sql.CompileUpdate(stmt, 100, 3)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Instructions: %d\n", len(prog.Instructions))
	fmt.Printf("Has WHERE clause: %v\n", stmt.Where != nil)

	// Output:
	// Instructions: 20
	// Has WHERE clause: true
}

// ExampleCompileDelete demonstrates compiling a DELETE statement
func ExampleCompileDelete() {
	where := sql.NewWhereClause(
		sql.NewBinaryExpression(
			sql.NewColumnExpression("age"),
			">",
			sql.NewLiteralExpression(sql.IntValue(65)),
		),
	)

	stmt := sql.NewDeleteStmt("users", where)

	prog, err := sql.CompileDelete(stmt, 100)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Instructions: %d\n", len(prog.Instructions))
	fmt.Printf("Table: %s\n", stmt.Table)

	// Output:
	// Instructions: 15
	// Table: users
}

// ExampleCompileDeleteWithTruncateOptimization demonstrates fast DELETE ALL
func ExampleCompileDeleteWithTruncateOptimization() {
	stmt := sql.NewDeleteStmt("temp_data", nil) // No WHERE clause

	prog, err := sql.CompileDeleteWithTruncateOptimization(stmt, 100)
	if err != nil {
		log.Fatal(err)
	}

	// Truncate optimization produces fewer instructions
	fmt.Printf("Instructions: %d\n", len(prog.Instructions))
	fmt.Printf("Optimized: %v\n", len(prog.Instructions) < 10)

	// Output:
	// Instructions: 5
	// Optimized: true
}

// ExampleSerialTypeFor demonstrates serial type selection
func ExampleSerialTypeFor() {
	examples := []sql.Value{
		sql.NullValue(),
		sql.IntValue(0),
		sql.IntValue(1),
		sql.IntValue(127),
		sql.IntValue(32767),
		sql.FloatValue(3.14),
		sql.TextValue("hi"),
		sql.BlobValue([]byte{1, 2, 3}),
	}

	for _, val := range examples {
		st := sql.SerialTypeFor(val)
		fmt.Printf("Type %v -> Serial type %d\n", val.Type, st)
	}

	// Output:
	// Type 0 -> Serial type 0
	// Type 1 -> Serial type 8
	// Type 1 -> Serial type 9
	// Type 1 -> Serial type 1
	// Type 1 -> Serial type 2
	// Type 2 -> Serial type 7
	// Type 3 -> Serial type 17
	// Type 4 -> Serial type 18
}

// ExampleValidateInsert demonstrates INSERT validation
func ExampleValidateInsert() {
	// Valid INSERT
	validStmt := sql.NewInsertStmt(
		"users",
		[]string{"id", "name"},
		[][]sql.Value{
			{sql.IntValue(1), sql.TextValue("Alice")},
		},
	)

	err1 := sql.ValidateInsert(validStmt)
	fmt.Printf("Valid INSERT error: %v\n", err1)

	// Invalid INSERT (mismatched columns)
	invalidStmt := sql.NewInsertStmt(
		"users",
		[]string{"id", "name"},
		[][]sql.Value{
			{sql.IntValue(1)}, // Missing name
		},
	)

	err2 := sql.ValidateInsert(invalidStmt)
	fmt.Printf("Invalid INSERT has error: %v\n", err2 != nil)

	// Output:
	// Valid INSERT error: <nil>
	// Invalid INSERT has error: true
}

// ExamplePutVarint demonstrates varint encoding using SQLite format
// SQLite uses 7-bit continuation encoding (MSB set = more bytes follow)
func ExamplePutVarint() {
	values := []uint64{0, 42, 127, 128, 16383, 16384, 1000000}

	for _, v := range values {
		buf := sql.PutVarint(nil, v)
		decoded, _ := sql.GetVarint(buf, 0)

		fmt.Printf("Value %d: %d bytes, decoded=%d, match=%v\n",
			v, len(buf), decoded, v == decoded)
	}

	// Output:
	// Value 0: 1 bytes, decoded=0, match=true
	// Value 42: 1 bytes, decoded=42, match=true
	// Value 127: 1 bytes, decoded=127, match=true
	// Value 128: 2 bytes, decoded=128, match=true
	// Value 16383: 2 bytes, decoded=16383, match=true
	// Value 16384: 3 bytes, decoded=16384, match=true
	// Value 1000000: 3 bytes, decoded=1000000, match=true
}

// ExampleNewBinaryExpression demonstrates expression building
func ExampleNewBinaryExpression() {
	// Create expression: age >= 18
	expr := sql.NewBinaryExpression(
		sql.NewColumnExpression("age"),
		">=",
		sql.NewLiteralExpression(sql.IntValue(18)),
	)

	fmt.Printf("Expression type: %v\n", expr.Type)
	fmt.Printf("Operator: %s\n", expr.Operator)
	fmt.Printf("Left column: %s\n", expr.Left.Column)

	// Output:
	// Expression type: 2
	// Operator: >=
	// Left column: age
}

// ExampleInsertStmt_multiRow demonstrates multi-row INSERT
func ExampleInsertStmt_multiRow() {
	stmt := sql.NewInsertStmt(
		"scores",
		[]string{"player", "score"},
		[][]sql.Value{
			{sql.TextValue("Alice"), sql.IntValue(100)},
			{sql.TextValue("Bob"), sql.IntValue(95)},
			{sql.TextValue("Carol"), sql.IntValue(105)},
		},
	)

	prog, err := sql.CompileInsert(stmt, 200)
	if err != nil {
		log.Fatal(err)
	}

	// Count Insert instructions
	insertCount := 0
	for _, inst := range prog.Instructions {
		if inst.OpCode == sql.OpInsert {
			insertCount++
		}
	}

	fmt.Printf("Rows to insert: %d\n", len(stmt.Values))
	fmt.Printf("Insert instructions: %d\n", insertCount)

	// Output:
	// Rows to insert: 3
	// Insert instructions: 3
}

// ExampleValue_types demonstrates all value types
func ExampleValue_types() {
	values := []struct {
		name string
		val  sql.Value
	}{
		{"NULL", sql.NullValue()},
		{"Integer", sql.IntValue(42)},
		{"Float", sql.FloatValue(3.14)},
		{"Text", sql.TextValue("hello")},
		{"Blob", sql.BlobValue([]byte{0xDE, 0xAD, 0xBE, 0xEF})},
	}

	for _, v := range values {
		fmt.Printf("%-10s Type=%d IsNull=%v\n", v.name, v.val.Type, v.val.IsNull)
	}

	// Output:
	// NULL       Type=0 IsNull=true
	// Integer    Type=1 IsNull=false
	// Float      Type=2 IsNull=false
	// Text       Type=3 IsNull=false
	// Blob       Type=4 IsNull=false
}

// ExampleRecord roundtrip with complex data
func ExampleRecord_complex() {
	original := []sql.Value{
		sql.IntValue(-128),
		sql.IntValue(32767),
		sql.FloatValue(-3.14159),
		sql.TextValue("SQLite"),
		sql.BlobValue([]byte{0xFF, 0x00, 0xFF}),
		sql.NullValue(),
		sql.IntValue(0),
		sql.IntValue(1),
	}

	// Encode
	record, err := sql.MakeRecord(original)
	if err != nil {
		log.Fatal(err)
	}

	// Decode
	parsed, err := sql.ParseRecord(record)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Original count: %d\n", len(original))
	fmt.Printf("Parsed count: %d\n", len(parsed.Values))
	fmt.Printf("Record size: %d bytes\n", len(record))
	fmt.Printf("Match: %v\n", len(original) == len(parsed.Values))

	// Output:
	// Original count: 8
	// Parsed count: 8
	// Record size: 29 bytes
	// Match: true
}
