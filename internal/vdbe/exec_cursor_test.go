package vdbe

import (
	"encoding/binary"
	"testing"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
)

// makeSimpleRecord creates a simple SQLite record for testing
func makeSimpleRecord(intVal int64, textVal string) []byte {
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

	textSerial := byte(13 + 2*len(textVal))
	textBytes := []byte(textVal)

	record := make([]byte, 0, 100)
	headerSize := byte(1 + 1 + 1)
	record = append(record, headerSize)
	record = append(record, intSerial)
	record = append(record, textSerial)
	record = append(record, intBytes...)
	record = append(record, textBytes...)

	return record
}

// createSeekTestBtree creates a btree with specific rowids for testing seek operations
func createSeekTestBtree() *btree.Btree {
	bt := btree.NewBtree(4096)

	pageData := make([]byte, 4096)

	// SQLite file header (100 bytes) for page 1
	copy(pageData[0:16], []byte("SQLite format 3\x00"))
	pageData[18] = 16
	pageData[19] = 0

	// Page header starts at offset 100 for page 1
	hdr := 100
	pageData[hdr+0] = 0x0d // Page type: table leaf
	pageData[hdr+1] = 0x00
	pageData[hdr+2] = 0x00
	pageData[hdr+3] = 0x00
	pageData[hdr+4] = 0x05 // Number of cells = 5
	pageData[hdr+5] = 0x00
	pageData[hdr+6] = 0xc8 // Cell content offset = 200
	pageData[hdr+7] = 0x00

	// Cell pointer array (5 cells)
	pageData[hdr+8] = 0x00
	pageData[hdr+9] = 0xc8 // Cell 1 at offset 200, rowid=10
	pageData[hdr+10] = 0x00
	pageData[hdr+11] = 0xf0 // Cell 2 at offset 240, rowid=20
	pageData[hdr+12] = 0x01
	pageData[hdr+13] = 0x18 // Cell 3 at offset 280, rowid=30
	pageData[hdr+14] = 0x01
	pageData[hdr+15] = 0x40 // Cell 4 at offset 320, rowid=40
	pageData[hdr+16] = 0x01
	pageData[hdr+17] = 0x68 // Cell 5 at offset 360, rowid=50

	// Create cells with rowids: 10, 20, 30, 40, 50
	createCell := func(offset, rowid int, data string) {
		record := makeSimpleRecord(int64(rowid*10), data)
		pageData[offset] = byte(len(record))
		offset++
		pageData[offset] = byte(rowid)
		offset++
		copy(pageData[offset:], record)
	}

	createCell(200, 10, "A")
	createCell(240, 20, "B")
	createCell(280, 30, "C")
	createCell(320, 40, "D")
	createCell(360, 50, "E")

	bt.SetPage(1, pageData)
	return bt
}

func TestOpOpenEphemeral(t *testing.T) {
	v := New()
	v.AllocMemory(10)
	v.AllocCursors(5)

	// Open ephemeral table with cursor 0, 3 columns
	v.AddOp(OpOpenEphemeral, 0, 3, 0)
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

	if cursor.CurType != CursorPseudo {
		t.Errorf("Expected CursorPseudo for ephemeral table, got %v", cursor.CurType)
	}

	if !cursor.Writable {
		t.Error("Expected ephemeral cursor to be writable")
	}
}

func TestOpSeekGT(t *testing.T) {
	bt := createSeekTestBtree()

	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}
	v.AllocMemory(10)
	v.AllocCursors(5)

	// Program:
	// OpenRead cursor 0
	// Integer 25 -> r1 (key)
	// SeekGT cursor 0, key in r1, jump to NotFound if not found
	// Rowid cursor 0 -> r2 (should be 30, first rowid > 25)
	// Goto End
	// NotFound: Integer -1 -> r2
	// End: Halt

	v.AddOp(OpOpenRead, 0, 1, 2)   // 0
	v.AddOp(OpInteger, 25, 1, 0)   // 1: r1 = 25
	v.AddOp(OpSeekGT, 0, 6, 1)     // 2: seek > 25, jump to 6 if not found
	v.AddOp(OpRowid, 0, 2, 0)      // 3: r2 = rowid
	v.AddOp(OpGoto, 0, 7, 0)       // 4: goto 7
	v.AddOp(OpInteger, -1, 2, 0)   // 5: not used (spacing)
	v.AddOp(OpInteger, -1, 2, 0)   // 6: r2 = -1 (not found)
	v.AddOp(OpHalt, 0, 0, 0)       // 7

	err := v.Run()
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	r2, _ := v.GetMem(2)
	if r2.IntValue() != 30 {
		t.Errorf("Expected rowid 30 (first > 25), got %d", r2.IntValue())
	}
}

func TestOpSeekGTNotFound(t *testing.T) {
	bt := createSeekTestBtree()

	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}
	v.AllocMemory(10)
	v.AllocCursors(5)

	// Seek for rowid > 50 (should not find anything)
	v.AddOp(OpOpenRead, 0, 1, 2)
	v.AddOp(OpInteger, 50, 1, 0)   // r1 = 50
	v.AddOp(OpSeekGT, 0, 5, 1)     // jump to 5 if not found
	v.AddOp(OpInteger, 1, 2, 0)    // r2 = 1 (found - shouldn't execute)
	v.AddOp(OpGoto, 0, 6, 0)
	v.AddOp(OpInteger, 0, 2, 0)    // r2 = 0 (not found)
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

func TestOpSeekLT(t *testing.T) {
	bt := createSeekTestBtree()

	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}
	v.AllocMemory(10)
	v.AllocCursors(5)

	// Program:
	// OpenRead cursor 0
	// Integer 35 -> r1 (key)
	// SeekLT cursor 0, key in r1, jump to NotFound if not found
	// Rowid cursor 0 -> r2 (should be 30, last rowid < 35)
	// Goto End
	// NotFound: Integer -1 -> r2
	// End: Halt

	v.AddOp(OpOpenRead, 0, 1, 2)
	v.AddOp(OpInteger, 35, 1, 0)   // r1 = 35
	v.AddOp(OpSeekLT, 0, 6, 1)     // seek < 35, jump to 6 if not found
	v.AddOp(OpRowid, 0, 2, 0)      // r2 = rowid
	v.AddOp(OpGoto, 0, 7, 0)
	v.AddOp(OpInteger, -1, 2, 0)   // spacing
	v.AddOp(OpInteger, -1, 2, 0)   // r2 = -1 (not found)
	v.AddOp(OpHalt, 0, 0, 0)

	err := v.Run()
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	r2, _ := v.GetMem(2)
	if r2.IntValue() != 30 {
		t.Errorf("Expected rowid 30 (last < 35), got %d", r2.IntValue())
	}
}

func TestOpSeekLTNotFound(t *testing.T) {
	bt := createSeekTestBtree()

	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}
	v.AllocMemory(10)
	v.AllocCursors(5)

	// Seek for rowid < 10 (should not find anything)
	v.AddOp(OpOpenRead, 0, 1, 2)
	v.AddOp(OpInteger, 10, 1, 0)   // r1 = 10
	v.AddOp(OpSeekLT, 0, 5, 1)     // jump to 5 if not found
	v.AddOp(OpInteger, 1, 2, 0)    // r2 = 1 (found - shouldn't execute)
	v.AddOp(OpGoto, 0, 6, 0)
	v.AddOp(OpInteger, 0, 2, 0)    // r2 = 0 (not found)
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

func TestOpNotExists(t *testing.T) {
	bt := createSeekTestBtree()

	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}
	v.AllocMemory(10)
	v.AllocCursors(5)

	// Test 1: Check for existing rowid (30) - should NOT jump
	v.AddOp(OpOpenRead, 0, 1, 2)
	v.AddOp(OpInteger, 30, 1, 0)   // r1 = 30 (exists)
	v.AddOp(OpNotExists, 0, 5, 1)  // jump to 5 if NOT exists
	v.AddOp(OpInteger, 1, 2, 0)    // r2 = 1 (exists)
	v.AddOp(OpGoto, 0, 6, 0)
	v.AddOp(OpInteger, 0, 2, 0)    // r2 = 0 (doesn't exist)
	v.AddOp(OpHalt, 0, 0, 0)

	err := v.Run()
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	r2, _ := v.GetMem(2)
	if r2.IntValue() != 1 {
		t.Errorf("Expected r2=1 (exists), got %d", r2.IntValue())
	}
}

func TestOpNotExistsJumps(t *testing.T) {
	bt := createSeekTestBtree()

	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}
	v.AllocMemory(10)
	v.AllocCursors(5)

	// Test 2: Check for non-existing rowid (25) - should jump
	v.AddOp(OpOpenRead, 0, 1, 2)
	v.AddOp(OpInteger, 25, 1, 0)   // r1 = 25 (doesn't exist)
	v.AddOp(OpNotExists, 0, 5, 1)  // jump to 5 if NOT exists
	v.AddOp(OpInteger, 1, 2, 0)    // r2 = 1 (exists - shouldn't execute)
	v.AddOp(OpGoto, 0, 6, 0)
	v.AddOp(OpInteger, 0, 2, 0)    // r2 = 0 (doesn't exist)
	v.AddOp(OpHalt, 0, 0, 0)

	err := v.Run()
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	r2, _ := v.GetMem(2)
	if r2.IntValue() != 0 {
		t.Errorf("Expected r2=0 (doesn't exist), got %d", r2.IntValue())
	}
}

func TestOpDeferredSeek(t *testing.T) {
	bt := createSeekTestBtree()

	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}
	v.AllocMemory(10)
	v.AllocCursors(5)

	// Program:
	// OpenRead cursor 0 (index cursor - not used in this simple test)
	// OpenRead cursor 1 (table cursor)
	// Integer 30 -> r1 (rowid to seek)
	// DeferredSeek: index cursor=0, table cursor=1, rowid in r1
	// Column from cursor 1 -> r2
	// Halt

	v.AddOp(OpOpenRead, 0, 1, 2)     // 0: open index cursor (dummy)
	v.AddOp(OpOpenRead, 1, 1, 2)     // 1: open table cursor
	v.AddOp(OpInteger, 30, 1, 0)     // 2: r1 = 30
	v.AddOp(OpDeferredSeek, 0, 1, 1) // 3: deferred seek on cursor 1 to rowid in r1
	v.AddOp(OpRowid, 1, 2, 0)        // 4: r2 = rowid from cursor 1
	v.AddOp(OpHalt, 0, 0, 0)         // 5

	err := v.Run()
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	r2, _ := v.GetMem(2)
	if r2.IntValue() != 30 {
		t.Errorf("Expected rowid 30, got %d", r2.IntValue())
	}

	// Verify cursor 1 is not at EOF
	cursor1, _ := v.GetCursor(1)
	if cursor1.EOF {
		t.Error("Expected cursor 1 to not be at EOF after successful deferred seek")
	}
}

func TestOpDeferredSeekNotFound(t *testing.T) {
	bt := createSeekTestBtree()

	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}
	v.AllocMemory(10)
	v.AllocCursors(5)

	// Seek for non-existing rowid 99
	v.AddOp(OpOpenRead, 0, 1, 2)
	v.AddOp(OpOpenRead, 1, 1, 2)
	v.AddOp(OpInteger, 99, 1, 0)     // r1 = 99 (doesn't exist)
	v.AddOp(OpDeferredSeek, 0, 1, 1)
	v.AddOp(OpHalt, 0, 0, 0)

	err := v.Run()
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	// Verify cursor 1 is at EOF
	cursor1, _ := v.GetCursor(1)
	if !cursor1.EOF {
		t.Error("Expected cursor 1 to be at EOF after failed deferred seek")
	}
}

func TestOpSeekGTBoundary(t *testing.T) {
	bt := createSeekTestBtree()

	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}
	v.AllocMemory(10)
	v.AllocCursors(5)

	// Seek for rowid > 15 (should find 20)
	v.AddOp(OpOpenRead, 0, 1, 2)
	v.AddOp(OpInteger, 15, 1, 0)
	v.AddOp(OpSeekGT, 0, 5, 1)
	v.AddOp(OpRowid, 0, 2, 0)
	v.AddOp(OpGoto, 0, 6, 0)
	v.AddOp(OpInteger, -1, 2, 0)
	v.AddOp(OpHalt, 0, 0, 0)

	err := v.Run()
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	r2, _ := v.GetMem(2)
	if r2.IntValue() != 20 {
		t.Errorf("Expected rowid 20 (first > 15), got %d", r2.IntValue())
	}
}

func TestOpSeekLTBoundary(t *testing.T) {
	bt := createSeekTestBtree()

	v := New()
	v.Ctx = &VDBEContext{
		Btree: bt,
	}
	v.AllocMemory(10)
	v.AllocCursors(5)

	// Seek for rowid < 45 (should find 40)
	v.AddOp(OpOpenRead, 0, 1, 2)
	v.AddOp(OpInteger, 45, 1, 0)
	v.AddOp(OpSeekLT, 0, 5, 1)
	v.AddOp(OpRowid, 0, 2, 0)
	v.AddOp(OpGoto, 0, 6, 0)
	v.AddOp(OpInteger, -1, 2, 0)
	v.AddOp(OpHalt, 0, 0, 0)

	err := v.Run()
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	r2, _ := v.GetMem(2)
	if r2.IntValue() != 40 {
		t.Errorf("Expected rowid 40 (last < 45), got %d", r2.IntValue())
	}
}
