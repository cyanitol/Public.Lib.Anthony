package vdbe

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
)

// makeTestRecord creates a simple SQLite record for testing
// Format: header_size, serial_types..., values...
func makeTestRecord(intVal int64, textVal string) []byte {
	// Serial type for int: depends on value
	var intSerial byte = 1 // INT8
	var intBytes []byte
	if intVal >= -128 && intVal <= 127 {
		intSerial = 1 // INT8
		intBytes = []byte{byte(intVal)}
	} else {
		intSerial = 6 // INT64
		intBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(intBytes, uint64(intVal))
	}

	// Serial type for text: 13 + 2*len
	textSerial := byte(13 + 2*len(textVal))
	textBytes := []byte(textVal)

	// Build record
	record := make([]byte, 0, 100)
	// Header size (2 + serial types)
	headerSize := byte(1 + 1 + 1) // header_size_byte + int_serial + text_serial
	record = append(record, headerSize)
	record = append(record, intSerial)
	record = append(record, textSerial)
	// Values
	record = append(record, intBytes...)
	record = append(record, textBytes...)

	return record
}

// createTestBtree creates a btree with test data
func createTestBtree() *btree.Btree {
	bt := btree.NewBtree(4096)

	// Create a simple leaf page with table data
	// Page format: [100-byte SQLite file header][page header][cell pointers][cells]
	pageData := make([]byte, 4096)

	// SQLite file header (100 bytes) for page 1
	copy(pageData[0:16], []byte("SQLite format 3\x00"))
	pageData[18] = 16 // Page size / 256 (4096/256=16)
	pageData[19] = 0
	// Other header fields can be zero for this test

	// Page header starts at offset 100 for page 1 (13 bytes)
	hdr := 100
	pageData[hdr+0] = 0x0d // Page type: table leaf
	pageData[hdr+1] = 0x00 // First freeblock offset (high byte)
	pageData[hdr+2] = 0x00 // First freeblock offset (low byte)
	pageData[hdr+3] = 0x00 // Number of cells (high byte)
	pageData[hdr+4] = 0x03 // Number of cells (low byte) = 3 cells
	pageData[hdr+5] = 0x00 // Cell content offset (high byte)
	pageData[hdr+6] = 0xc8 // Cell content offset (low byte) = 200
	pageData[hdr+7] = 0x00 // Fragmented free bytes

	// Cell pointer array (2 bytes per cell, 3 cells = 6 bytes)
	// Starts after page header at offset 108
	// Cell 1 at offset 200
	pageData[hdr+8] = 0x00
	pageData[hdr+9] = 0xc8
	// Cell 2 at offset 250
	pageData[hdr+10] = 0x00
	pageData[hdr+11] = 0xfa
	// Cell 3 at offset 300
	pageData[hdr+12] = 0x01
	pageData[hdr+13] = 0x2c

	// Create cell 1: rowid=1, record with two columns (int 42, text "Alice")
	record1 := makeTestRecord(42, "Alice")
	offset := 200
	// Payload size varint
	pageData[offset] = byte(len(record1))
	offset++
	// Rowid varint
	pageData[offset] = 1
	offset++
	// Payload
	copy(pageData[offset:], record1)

	// Create cell 2: rowid=2, record with two columns (int 99, text "Bob")
	record2 := makeTestRecord(99, "Bob")
	offset = 250
	pageData[offset] = byte(len(record2))
	offset++
	pageData[offset] = 2
	offset++
	copy(pageData[offset:], record2)

	// Create cell 3: rowid=3, record with two columns (int 123, text "Charlie")
	record3 := makeTestRecord(123, "Charlie")
	offset = 300
	pageData[offset] = byte(len(record3))
	offset++
	pageData[offset] = 3
	offset++
	copy(pageData[offset:], record3)

	// Set the page
	bt.SetPage(1, pageData)

	return bt
}

// Suppress unused import warnings
var _ = math.Float64frombits
var _ = binary.BigEndian

func TestCursorOpenRead(t *testing.T) {
	bt := createTestBtree()

	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}
	v.AllocMemory(10)
	v.AllocCursors(5)

	// OpenRead cursor 0 on root page 1
	v.AddOp(OpOpenRead, 0, 1, 2) // cursor 0, root page 1, 2 columns
	v.AddOp(OpHalt, 0, 0, 0)

	err := v.Run()
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	// Verify cursor was opened
	cursor, err := v.GetCursor(0)
	if err != nil {
		t.Fatalf("Failed to get cursor: %v", err)
	}

	if cursor.CurType != CursorBTree {
		t.Errorf("Expected CursorBTree, got %v", cursor.CurType)
	}

	if cursor.RootPage != 1 {
		t.Errorf("Expected root page 1, got %d", cursor.RootPage)
	}
}

func TestCursorRewindAndNext(t *testing.T) {
	bt := createTestBtree()

	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}
	v.AllocMemory(10)
	v.AllocCursors(5)

	// Program:
	// OpenRead cursor 0 on page 1
	// Rewind cursor 0 to beginning, jump to Halt if empty
	// Loop: Column 0,0,1 (get first column into r1)
	//       Column 0,1,2 (get second column into r2)
	//       Next cursor 0, jump back to Loop if more rows
	// Halt

	v.AddOp(OpOpenRead, 0, 1, 2) // 0: OpenRead cursor 0, page 1
	v.AddOp(OpRewind, 0, 9, 0)   // 1: Rewind cursor 0, jump to 9 if empty
	// Loop starts at 2
	v.AddOp(OpColumn, 0, 0, 1) // 2: r1 = cursor[0].col[0]
	v.AddOp(OpColumn, 0, 1, 2) // 3: r2 = cursor[0].col[1]
	v.AddOp(OpNext, 0, 2, 0)   // 4: Next cursor 0, jump to 2 if more
	v.AddOp(OpHalt, 0, 0, 0)   // 5: Halt

	err := v.Run()
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	// After execution, registers should contain the last row's data
	r1, _ := v.GetMem(1)
	r2, _ := v.GetMem(2)

	if !r1.IsInt() || r1.IntValue() != 123 {
		t.Errorf("Expected r1=123, got %v (value=%d)", r1, r1.IntValue())
	}

	if !r2.IsStr() || r2.StrValue() != "Charlie" {
		t.Errorf("Expected r2='Charlie', got %v (value=%s)", r2, r2.StrValue())
	}
}

func TestCursorRowid(t *testing.T) {
	bt := createTestBtree()

	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}
	v.AllocMemory(10)
	v.AllocCursors(5)

	// Program:
	// OpenRead cursor 0
	// Rewind cursor 0
	// Rowid cursor 0 -> r1
	// Next cursor 0
	// Rowid cursor 0 -> r2
	// Halt

	v.AddOp(OpOpenRead, 0, 1, 2)
	v.AddOp(OpRewind, 0, 8, 0)
	v.AddOp(OpRowid, 0, 1, 0) // r1 = rowid
	v.AddOp(OpNext, 0, 4, 0)
	v.AddOp(OpRowid, 0, 2, 0) // r2 = rowid
	v.AddOp(OpHalt, 0, 0, 0)

	err := v.Run()
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	r1, _ := v.GetMem(1)
	r2, _ := v.GetMem(2)

	if r1.IntValue() != 1 {
		t.Errorf("Expected first rowid=1, got %d", r1.IntValue())
	}

	if r2.IntValue() != 2 {
		t.Errorf("Expected second rowid=2, got %d", r2.IntValue())
	}
}

func TestCursorSeekRowid(t *testing.T) {
	bt := createTestBtree()

	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}
	v.AllocMemory(10)
	v.AllocCursors(5)

	// Program:
	// OpenRead cursor 0
	// Integer 2 -> r1 (target rowid)
	// SeekRowid cursor 0, r1, jump to NotFound if not found
	// Column 0,0 -> r2 (should be 99)
	// Column 0,1 -> r3 (should be "Bob")
	// Goto End
	// NotFound: Integer -1 -> r2
	// End: Halt

	v.AddOp(OpOpenRead, 0, 1, 2)  // 0
	v.AddOp(OpInteger, 2, 1, 0)   // 1: r1 = 2
	v.AddOp(OpSeekRowid, 0, 6, 1) // 2: seek cursor 0 to rowid in r1, jump to 6 if not found
	v.AddOp(OpColumn, 0, 0, 2)    // 3: r2 = col 0
	v.AddOp(OpColumn, 0, 1, 3)    // 4: r3 = col 1
	v.AddOp(OpGoto, 0, 7, 0)      // 5: goto 7
	v.AddOp(OpInteger, -1, 2, 0)  // 6: r2 = -1 (not found)
	v.AddOp(OpHalt, 0, 0, 0)      // 7

	err := v.Run()
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	r2, _ := v.GetMem(2)
	r3, _ := v.GetMem(3)

	if r2.IntValue() != 99 {
		t.Errorf("Expected r2=99, got %d", r2.IntValue())
	}

	if r3.StrValue() != "Bob" {
		t.Errorf("Expected r3='Bob', got %s", r3.StrValue())
	}
}

func TestCursorSeekRowidNotFound(t *testing.T) {
	bt := createTestBtree()

	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}
	v.AllocMemory(10)
	v.AllocCursors(5)

	// Seek for non-existent rowid 99
	v.AddOp(OpOpenRead, 0, 1, 2)
	v.AddOp(OpInteger, 99, 1, 0)  // r1 = 99 (doesn't exist)
	v.AddOp(OpSeekRowid, 0, 5, 1) // jump to 5 if not found
	v.AddOp(OpInteger, 1, 2, 0)   // r2 = 1 (found)
	v.AddOp(OpGoto, 0, 6, 0)      // goto 6
	v.AddOp(OpInteger, 0, 2, 0)   // r2 = 0 (not found)
	v.AddOp(OpHalt, 0, 0, 0)

	err := v.Run()
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	r2, _ := v.GetMem(2)
	if r2.IntValue() != 0 {
		t.Errorf("Expected r2=0 (not found), got %d", r2.IntValue())
	}
}

func TestCursorClose(t *testing.T) {
	bt := createTestBtree()

	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}
	v.AllocMemory(10)
	v.AllocCursors(5)

	// Open and close cursor
	v.AddOp(OpOpenRead, 0, 1, 2)
	v.AddOp(OpClose, 0, 0, 0)
	v.AddOp(OpHalt, 0, 0, 0)

	err := v.Run()
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	// Verify cursor is closed
	_, err = v.GetCursor(0)
	if err == nil {
		t.Error("Expected error when getting closed cursor")
	}
}

func TestCursorPrev(t *testing.T) {
	bt := createTestBtree()

	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}
	v.AllocMemory(10)
	v.AllocCursors(5)

	// Program:
	// Open cursor, seek to rowid 3, then go backwards
	v.AddOp(OpOpenRead, 0, 1, 2)
	v.AddOp(OpInteger, 3, 1, 0)    // r1 = 3
	v.AddOp(OpSeekRowid, 0, 10, 1) // seek to rowid 3
	v.AddOp(OpRowid, 0, 2, 0)      // r2 = rowid (should be 3)
	v.AddOp(OpPrev, 0, 6, 0)       // prev, jump to 6 if more
	v.AddOp(OpGoto, 0, 8, 0)       // goto end
	v.AddOp(OpRowid, 0, 3, 0)      // r3 = rowid (should be 2)
	v.AddOp(OpPrev, 0, 10, 0)      // prev again
	v.AddOp(OpHalt, 0, 0, 0)

	err := v.Run()
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	r2, _ := v.GetMem(2)
	r3, _ := v.GetMem(3)

	if r2.IntValue() != 3 {
		t.Errorf("Expected r2=3, got %d", r2.IntValue())
	}

	if r3.IntValue() != 2 {
		t.Errorf("Expected r3=2, got %d", r3.IntValue())
	}
}

func TestCursorEmptyTable(t *testing.T) {
	// Create empty btree
	bt := btree.NewBtree(4096)
	pageData := make([]byte, 4096)
	pageData[0] = 0x0d // Table leaf page
	pageData[3] = 0x00 // 0 cells
	pageData[4] = 0x00
	bt.SetPage(1, pageData)

	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}
	v.AllocMemory(10)
	v.AllocCursors(5)

	// Rewind should jump to halt when empty
	v.AddOp(OpOpenRead, 0, 1, 0)
	v.AddOp(OpRewind, 0, 4, 0)  // jump to 4 if empty
	v.AddOp(OpInteger, 1, 1, 0) // r1 = 1 (shouldn't execute)
	v.AddOp(OpGoto, 0, 5, 0)
	v.AddOp(OpInteger, 0, 1, 0) // r1 = 0 (should execute)
	v.AddOp(OpHalt, 0, 0, 0)

	err := v.Run()
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	r1, _ := v.GetMem(1)
	if r1.IntValue() != 0 {
		t.Errorf("Expected r1=0 (empty table path), got %d", r1.IntValue())
	}
}
