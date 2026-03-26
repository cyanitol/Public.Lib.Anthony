// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cyanitol/Public.Lib.Anthony/internal/withoutrowid"
)

// TestCellOverflowWithoutRowidComposite exercises parseTableLeafCompositeCell by
// inserting rows into a WITHOUT ROWID table using composite primary keys.
// Small page size (512) forces splits and covers multiple code paths.
func TestCellOverflowWithoutRowidComposite(t *testing.T) {
	t.Parallel()

	t.Run("basic composite cell round-trip", func(t *testing.T) {
		// Directly test parseTableLeafCompositeCell via ParseCell
		keyBytes := withoutrowid.EncodeCompositeKey([]interface{}{"region-west", "user-12345"})
		payload := bytes.Repeat([]byte("data"), 20)

		cell := EncodeTableLeafCompositeCell(keyBytes, payload)
		info, err := ParseCell(PageTypeLeafTableNoInt, cell, 4096)
		if err != nil {
			t.Fatalf("ParseCell(PageTypeLeafTableNoInt) error = %v", err)
		}
		if !bytes.Equal(info.KeyBytes, keyBytes) {
			t.Errorf("KeyBytes mismatch: got %v, want %v", info.KeyBytes, keyBytes)
		}
		if info.PayloadSize != uint32(len(payload)) {
			t.Errorf("PayloadSize = %d, want %d", info.PayloadSize, len(payload))
		}
	})

	t.Run("empty cell data error", func(t *testing.T) {
		_, err := ParseCell(PageTypeLeafTableNoInt, []byte{}, 4096)
		if err == nil {
			t.Error("ParseCell with empty data should return error")
		}
	})

	t.Run("key length exceeds cell size error", func(t *testing.T) {
		// Craft a cell where key length varint says more bytes than exist
		buf := make([]byte, 10)
		n := PutVarint(buf, 5)       // payloadSize = 5
		n += PutVarint(buf[n:], 200) // keyLen = 200, but only a few bytes remain
		_, err := ParseCell(PageTypeLeafTableNoInt, buf[:n+2], 4096)
		if err == nil {
			t.Error("ParseCell should fail when key length exceeds cell data")
		}
	})

	t.Run("composite pk insert many rows", func(t *testing.T) {
		bt := NewBtree(512)
		root, err := bt.CreateWithoutRowidTable()
		if err != nil {
			t.Fatalf("CreateWithoutRowidTable: %v", err)
		}
		cursor := NewCursorWithOptions(bt, root, true)
		payload := bytes.Repeat([]byte("v"), 30)

		const n = 50
		for i := 0; i < n; i++ {
			key := withoutrowid.EncodeCompositeKey([]interface{}{
				fmt.Sprintf("region-%03d", i%5),
				fmt.Sprintf("user-%06d", i),
			})
			if err := cursor.InsertWithComposite(0, key, payload); err != nil {
				t.Fatalf("InsertWithComposite(%d): %v", i, err)
			}
		}

		scan := NewCursorWithOptions(bt, cursor.RootPage, true)
		if err := scan.MoveToFirst(); err != nil {
			t.Fatalf("MoveToFirst: %v", err)
		}
		count := 0
		for scan.IsValid() {
			count++
			if err := scan.Next(); err != nil {
				break
			}
		}
		if count != n {
			t.Errorf("expected %d rows, got %d", n, count)
		}
	})

	t.Run("composite pk with large key values", func(t *testing.T) {
		bt := NewBtree(4096)
		root, err := bt.CreateWithoutRowidTable()
		if err != nil {
			t.Fatalf("CreateWithoutRowidTable: %v", err)
		}
		cursor := NewCursorWithOptions(bt, root, true)
		payload := bytes.Repeat([]byte("p"), 50)

		// Large key components to exercise key-length varint path
		for i := 0; i < 20; i++ {
			longPart := fmt.Sprintf("%-100s", fmt.Sprintf("key-component-%d", i))
			key := withoutrowid.EncodeCompositeKey([]interface{}{longPart, int64(i)})
			if err := cursor.InsertWithComposite(0, key, payload); err != nil {
				t.Fatalf("InsertWithComposite(%d): %v", i, err)
			}
		}

		scan := NewCursorWithOptions(bt, cursor.RootPage, true)
		count := 0
		if err := scan.MoveToFirst(); err != nil {
			t.Fatalf("MoveToFirst: %v", err)
		}
		for scan.IsValid() {
			count++
			if err := scan.Next(); err != nil {
				break
			}
		}
		if count != 20 {
			t.Errorf("expected 20 rows, got %d", count)
		}
	})
}

// TestCellOverflowParseLeafCellHeader exercises parseLeafCellHeader error paths
// that are only reachable when the rowid varint cannot be read.
func TestCellOverflowParseLeafCellHeader(t *testing.T) {
	t.Parallel()

	t.Run("overflow payload size rejected", func(t *testing.T) {
		// Construct a cell where the payload size varint encodes a value > math.MaxUint32.
		// Varint 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0xFF 0x7F = very large value
		// We need payloadSize > MaxUint32 which requires 9 bytes all set.
		// Build a 9-byte varint that exceeds uint32 max.
		var buf [20]byte
		// Encode 2^33 as a varint (needs 5 bytes minimum, value > MaxUint32)
		val := uint64(1) << 33
		n := PutVarint(buf[:], val)
		// Add a dummy rowid byte
		buf[n] = 0x01
		_, err := ParseCell(PageTypeLeafTable, buf[:n+1], 4096)
		if err == nil {
			t.Error("ParseCell should fail for payload size > MaxUint32")
		}
	})

	t.Run("rowid varint missing", func(t *testing.T) {
		// Only one byte that is a complete varint for payload size but nothing after
		var buf [5]byte
		n := PutVarint(buf[:], 100) // payload size = 100
		// No bytes after the payload size varint → rowid read will fail
		_, err := ParseCell(PageTypeLeafTable, buf[:n], 4096)
		if err == nil {
			t.Error("ParseCell should fail when rowid varint is missing")
		}
	})

	t.Run("rowid varint overflow rejected", func(t *testing.T) {
		// Payload size varint then rowid varint that exceeds MaxInt64.
		// A 9-byte all-0xFF varint encodes the maximum uint64 value.
		var buf [20]byte
		n := PutVarint(buf[:], 10) // payload size = 10
		// All 9 bytes of 0xFF encodes math.MaxUint64, which > MaxInt64
		for i := 0; i < 9; i++ {
			buf[n+i] = 0xFF
		}
		_, err := ParseCell(PageTypeLeafTable, buf[:n+9], 4096)
		if err == nil {
			t.Error("ParseCell should fail for rowid > MaxInt64")
		}
	})
}

// TestCellOverflowExtractIndexPayloadAndOverflow exercises extractIndexPayloadAndOverflow
// by crafting index cells with overflow scenarios.
func TestCellOverflowExtractIndexPayloadAndOverflow(t *testing.T) {
	t.Parallel()

	t.Run("index cell data truncated error", func(t *testing.T) {
		// Build an index interior cell where payload claims more local bytes than exist.
		// Use a small cell buffer that is shorter than localPayload says.
		usableSize := uint32(4096)
		maxLocal := calculateMaxLocal(usableSize, false)

		info := &CellInfo{}
		// PayloadSize that exceeds maxLocal to trigger overflow path
		info.PayloadSize = maxLocal + 100
		// computeIndexCellSizeAndLocal would set LocalPayload to some value
		minLocal := calculateMinLocal(usableSize, false)
		info.LocalPayload = calculateLocalPayload(info.PayloadSize, minLocal, maxLocal, usableSize)

		// Build a buffer that is shorter than offset+localPayload
		offset := 5                                               // simulate 5 bytes before payload area
		shortBuf := make([]byte, offset+int(info.LocalPayload)/2) // too short

		err := extractIndexPayloadAndOverflow(info, shortBuf, offset, maxLocal)
		if err == nil {
			t.Error("extractIndexPayloadAndOverflow should fail when cell data is truncated")
		}
	})

	t.Run("overflow page number truncated error", func(t *testing.T) {
		usableSize := uint32(4096)
		maxLocal := calculateMaxLocal(usableSize, false)
		minLocal := calculateMinLocal(usableSize, false)

		info := &CellInfo{}
		info.PayloadSize = maxLocal + 500 // triggers overflow
		info.LocalPayload = calculateLocalPayload(info.PayloadSize, minLocal, maxLocal, usableSize)

		offset := 1
		// Buffer has room for local payload but NOT the 4-byte overflow page number
		bufLen := offset + int(info.LocalPayload) // exactly fills local, no room for overflow ptr
		buf := make([]byte, bufLen)

		err := extractIndexPayloadAndOverflow(info, buf, offset, maxLocal)
		if err == nil {
			t.Error("extractIndexPayloadAndOverflow should fail when overflow page number truncated")
		}
	})

	t.Run("overflow page number read successfully", func(t *testing.T) {
		usableSize := uint32(4096)
		maxLocal := calculateMaxLocal(usableSize, false)
		minLocal := calculateMinLocal(usableSize, false)

		info := &CellInfo{}
		info.PayloadSize = maxLocal + 500
		info.LocalPayload = calculateLocalPayload(info.PayloadSize, minLocal, maxLocal, usableSize)

		offset := 1
		// Buffer has room for local payload AND 4-byte overflow page pointer
		buf := make([]byte, offset+int(info.LocalPayload)+4)
		// Write overflow page number
		buf[offset+int(info.LocalPayload)] = 0x00
		buf[offset+int(info.LocalPayload)+1] = 0x00
		buf[offset+int(info.LocalPayload)+2] = 0x00
		buf[offset+int(info.LocalPayload)+3] = 0x07 // page 7

		err := extractIndexPayloadAndOverflow(info, buf, offset, maxLocal)
		if err != nil {
			t.Fatalf("extractIndexPayloadAndOverflow unexpected error: %v", err)
		}
		if info.OverflowPage != 7 {
			t.Errorf("OverflowPage = %d, want 7", info.OverflowPage)
		}
	})
}

// TestCellOverflowCompleteIndexCellParse exercises completeIndexCellParse via
// ParseCell with index interior cells that include overflow.
func TestCellOverflowCompleteIndexCellParse(t *testing.T) {
	t.Parallel()

	t.Run("index interior cell with large payload triggers overflow paths", func(t *testing.T) {
		usableSize := uint32(4096)
		maxLocal := calculateMaxLocal(usableSize, false)

		// Payload larger than maxLocal to trigger overflow branch in completeIndexCellParse
		largePayload := make([]byte, int(maxLocal)+200)
		for i := range largePayload {
			largePayload[i] = byte(i % 251)
		}

		// Encode index interior cell: child page + payload size varint + local payload + overflow ptr
		childPage := uint32(42)
		localPayload := CalculateLocalPayload(uint32(len(largePayload)), usableSize, false)

		// Build the cell manually: 4-byte child page, varint(payloadSize), local bytes, 4-byte overflow ptr
		cellBuf := make([]byte, 4+9+int(localPayload)+4)
		cellBuf[0] = byte(childPage >> 24)
		cellBuf[1] = byte(childPage >> 16)
		cellBuf[2] = byte(childPage >> 8)
		cellBuf[3] = byte(childPage)
		n := PutVarint(cellBuf[4:], uint64(len(largePayload)))
		copy(cellBuf[4+n:], largePayload[:localPayload])
		// Write overflow page pointer (page 99)
		overflowOffset := 4 + n + int(localPayload)
		cellBuf[overflowOffset] = 0
		cellBuf[overflowOffset+1] = 0
		cellBuf[overflowOffset+2] = 0
		cellBuf[overflowOffset+3] = 99

		info, err := ParseCell(PageTypeInteriorIndex, cellBuf[:overflowOffset+4], usableSize)
		if err != nil {
			t.Fatalf("ParseCell error = %v", err)
		}
		if info.ChildPage != childPage {
			t.Errorf("ChildPage = %d, want %d", info.ChildPage, childPage)
		}
		if info.OverflowPage != 99 {
			t.Errorf("OverflowPage = %d, want 99", info.OverflowPage)
		}
		if info.PayloadSize != uint32(len(largePayload)) {
			t.Errorf("PayloadSize = %d, want %d", info.PayloadSize, len(largePayload))
		}
	})

	t.Run("index leaf cell with overflow payload", func(t *testing.T) {
		usableSize := uint32(4096)
		maxLocal := calculateMaxLocal(usableSize, false)
		localPayload := CalculateLocalPayload(uint32(maxLocal)+300, usableSize, false)

		// Build cell: varint(payloadSize), local bytes, overflow ptr
		payloadSize := uint64(maxLocal) + 300
		cellBuf := make([]byte, 9+int(localPayload)+4)
		n := PutVarint(cellBuf, payloadSize)
		// local payload bytes already zero
		cellBuf[n+int(localPayload)] = 0
		cellBuf[n+int(localPayload)+1] = 0
		cellBuf[n+int(localPayload)+2] = 0
		cellBuf[n+int(localPayload)+3] = 55 // overflow page 55

		info, err := ParseCell(PageTypeLeafIndex, cellBuf[:n+int(localPayload)+4], usableSize)
		if err != nil {
			t.Fatalf("ParseCell error = %v", err)
		}
		if info.OverflowPage != 55 {
			t.Errorf("OverflowPage = %d, want 55", info.OverflowPage)
		}
	})
}

// TestCellOverflowParseIndexInteriorCell exercises parseIndexInteriorCell by
// constructing multi-level index B-trees manually (the IndexCursor does not support
// automatic splits, so we build the page structures by hand).
func TestCellOverflowParseIndexInteriorCell(t *testing.T) {
	t.Parallel()

	// encodeIndexPayload is a local helper (mirrors the private function used in zero_coverage_test).
	encodePayload := func(key []byte, rowid int64) []byte {
		rowidVarint := make([]byte, 9)
		n := PutVarint(rowidVarint, uint64(rowid))
		out := make([]byte, len(key)+n)
		copy(out, key)
		copy(out[len(key):], rowidVarint[:n])
		return out
	}

	t.Run("manual multi-level index tree traversal hits interior pages", func(t *testing.T) {
		bt := NewBtree(4096)

		// Build two leaf pages (pages 2 and 3) and one interior root (page 1).
		leaf1 := createIndexLeafPage(2, 4096, []struct {
			key   []byte
			rowid int64
		}{
			{[]byte("alpha"), 1},
			{[]byte("bravo"), 2},
			{[]byte("charlie"), 3},
		})
		bt.SetPage(2, leaf1)

		leaf2 := createIndexLeafPage(3, 4096, []struct {
			key   []byte
			rowid int64
		}{
			{[]byte("delta"), 4},
			{[]byte("echo"), 5},
			{[]byte("foxtrot"), 6},
		})
		bt.SetPage(3, leaf2)

		interiorPayload := encodePayload([]byte("charlie"), 3)
		interior := createIndexInteriorPage(1, 4096, []struct {
			childPage uint32
			key       []byte
			rowid     int64
		}{
			{2, []byte("charlie"), 3},
		}, 3)
		_ = interiorPayload
		bt.SetPage(1, interior)

		cursor := NewIndexCursor(bt, 1)
		if err := cursor.MoveToFirst(); err != nil {
			t.Fatalf("MoveToFirst: %v", err)
		}

		count := 0
		for cursor.IsValid() {
			count++
			if err := cursor.NextIndex(); err != nil {
				break
			}
		}
		if count < 3 {
			t.Errorf("expected at least 3 entries from multi-level index, got %d", count)
		}
	})

	t.Run("manual interior index with many cells hits parseIndexInteriorCell repeatedly", func(t *testing.T) {
		bt := NewBtree(4096)
		const numLeaves = 8

		// Create leaf pages 2..numLeaves+1
		for i := 0; i < numLeaves; i++ {
			pageNum := uint32(i + 2)
			key1 := []byte(fmt.Sprintf("key-%03d-a", i*2))
			key2 := []byte(fmt.Sprintf("key-%03d-b", i*2+1))
			leaf := createIndexLeafPage(pageNum, 4096, []struct {
				key   []byte
				rowid int64
			}{
				{key1, int64(i*2 + 1)},
				{key2, int64(i*2 + 2)},
			})
			bt.SetPage(pageNum, leaf)
		}

		// Build interior root (page 1) with numLeaves-1 cells pointing to child pages 2..numLeaves
		interiorCells := make([]struct {
			childPage uint32
			key       []byte
			rowid     int64
		}, numLeaves-1)
		for i := 0; i < numLeaves-1; i++ {
			interiorCells[i] = struct {
				childPage uint32
				key       []byte
				rowid     int64
			}{
				childPage: uint32(i + 2),
				key:       []byte(fmt.Sprintf("key-%03d-b", i*2+1)),
				rowid:     int64(i*2 + 2),
			}
		}
		interior := createIndexInteriorPage(1, 4096, interiorCells, uint32(numLeaves+1))
		bt.SetPage(1, interior)

		cursor := NewIndexCursor(bt, 1)
		if err := cursor.MoveToFirst(); err != nil {
			t.Fatalf("MoveToFirst: %v", err)
		}

		count := 0
		for cursor.IsValid() {
			count++
			if err := cursor.NextIndex(); err != nil {
				break
			}
		}
		if count < numLeaves {
			t.Errorf("expected at least %d entries, got %d", numLeaves, count)
		}
	})

	t.Run("error on too small cell data", func(t *testing.T) {
		// parseIndexInteriorCell requires at least 4 bytes for child page
		_, err := ParseCell(PageTypeInteriorIndex, []byte{0x01, 0x02}, 4096)
		if err == nil {
			t.Error("ParseCell(PageTypeInteriorIndex) with 2 bytes should fail")
		}
	})

	t.Run("index interior cell with standard payload", func(t *testing.T) {
		payload := []byte("standard-key-value")
		cell := EncodeIndexInteriorCell(77, payload)
		info, err := ParseCell(PageTypeInteriorIndex, cell, 4096)
		if err != nil {
			t.Fatalf("ParseCell error = %v", err)
		}
		if info.ChildPage != 77 {
			t.Errorf("ChildPage = %d, want 77", info.ChildPage)
		}
		if !bytes.Equal(info.Payload, payload) {
			t.Errorf("Payload mismatch")
		}
	})

	t.Run("direct parse of many interior index cells", func(t *testing.T) {
		// Parse 100 distinct interior cells directly to hit all branches of parseIndexInteriorCell
		for i := 0; i < 100; i++ {
			payload := []byte(fmt.Sprintf("interior-key-%05d", i))
			cell := EncodeIndexInteriorCell(uint32(i+2), payload)
			info, err := ParseCell(PageTypeInteriorIndex, cell, 4096)
			if err != nil {
				t.Fatalf("ParseCell[%d] error = %v", i, err)
			}
			if info.ChildPage != uint32(i+2) {
				t.Errorf("[%d] ChildPage = %d, want %d", i, info.ChildPage, i+2)
			}
		}
	})
}

// TestCellOverflowCalculateLocalPayload exercises calculateLocalPayload boundary
// conditions: exactly at minLocal, just above, surplus <= maxLocal, surplus > maxLocal.
func TestCellOverflowCalculateLocalPayload(t *testing.T) {
	t.Parallel()

	usableSize := uint32(4096)
	maxLocal := calculateMaxLocal(usableSize, false)
	minLocal := calculateMinLocal(usableSize, false)

	t.Run("payload below minLocal returns minLocal", func(t *testing.T) {
		// payloadSize < minLocal → should return minLocal
		if minLocal == 0 {
			t.Skip("minLocal is 0 for this usableSize")
		}
		result := calculateLocalPayload(minLocal-1, minLocal, maxLocal, usableSize)
		if uint32(result) != minLocal {
			t.Errorf("calculateLocalPayload(minLocal-1) = %d, want %d", result, minLocal)
		}
	})

	t.Run("payload exactly at minLocal", func(t *testing.T) {
		result := calculateLocalPayload(minLocal, minLocal, maxLocal, usableSize)
		// surplus = minLocal + (minLocal-minLocal)%(usableSize-4) = minLocal + 0 = minLocal
		// minLocal <= maxLocal → returns minLocal
		if uint32(result) != minLocal {
			t.Errorf("calculateLocalPayload(minLocal) = %d, want %d", result, minLocal)
		}
	})

	t.Run("surplus just below maxLocal", func(t *testing.T) {
		// Choose payloadSize so surplus = minLocal + X where X < maxLocal-minLocal
		// surplus = minLocal + (payloadSize-minLocal) % (usableSize-4)
		// Set (payloadSize-minLocal) = maxLocal-minLocal-1 so surplus = maxLocal-1
		payloadSize := minLocal + (maxLocal - minLocal - 1)
		if payloadSize < minLocal {
			t.Skip("cannot craft payloadSize for this usableSize")
		}
		result := calculateLocalPayload(payloadSize, minLocal, maxLocal, usableSize)
		surplus := minLocal + (payloadSize-minLocal)%(usableSize-4)
		expected := surplus
		if surplus > maxLocal {
			expected = minLocal
		}
		if uint32(result) != expected {
			t.Errorf("calculateLocalPayload surplus-below-max = %d, want %d", result, expected)
		}
	})

	t.Run("surplus exactly at maxLocal", func(t *testing.T) {
		// surplus = minLocal + (payloadSize-minLocal) % (usableSize-4) = maxLocal
		// payloadSize-minLocal = maxLocal-minLocal, so payloadSize = maxLocal
		payloadSize := maxLocal
		result := calculateLocalPayload(payloadSize, minLocal, maxLocal, usableSize)
		surplus := minLocal + (payloadSize-minLocal)%(usableSize-4)
		if surplus <= maxLocal {
			if uint32(result) != surplus {
				t.Errorf("calculateLocalPayload(maxLocal) = %d, want surplus %d", result, surplus)
			}
		} else {
			if uint32(result) != minLocal {
				t.Errorf("calculateLocalPayload(maxLocal) = %d, want minLocal %d", result, minLocal)
			}
		}
	})

	t.Run("surplus exceeds maxLocal falls back to minLocal", func(t *testing.T) {
		// Choose payloadSize so surplus = minLocal + (payloadSize-minLocal)%(usableSize-4) > maxLocal.
		// usableSize-4 = 4092; maxLocal = 4061; minLocal = ~480 for 4096 pages.
		// We need surplus > maxLocal.
		// Pick payloadSize such that (payloadSize-minLocal) % 4092 = maxLocal-minLocal+1
		remainder := maxLocal - minLocal + 1
		payloadSize := minLocal + remainder
		if payloadSize <= maxLocal {
			// Already within maxLocal, try a larger value that wraps
			payloadSize = minLocal + uint32(usableSize-4) + remainder
		}
		result := calculateLocalPayload(payloadSize, minLocal, maxLocal, usableSize)
		surplus := minLocal + (payloadSize-minLocal)%(usableSize-4)
		if surplus > maxLocal {
			if uint32(result) != minLocal {
				t.Errorf("surplus>maxLocal: calculateLocalPayload = %d, want minLocal %d", result, minLocal)
			}
		}
	})

	t.Run("usableSize less than 4 returns minLocal", func(t *testing.T) {
		// usableSize < 4 triggers the early return with minLocal
		result := calculateLocalPayload(1000, 50, 900, 3)
		if result != 50 {
			t.Errorf("calculateLocalPayload with usableSize<4 = %d, want 50", result)
		}
	})

	t.Run("small usable size", func(t *testing.T) {
		smallUsable := uint32(512)
		smallMax := calculateMaxLocal(smallUsable, false)
		smallMin := calculateMinLocal(smallUsable, false)
		// payload much larger than max
		result := calculateLocalPayload(smallMax*10, smallMin, smallMax, smallUsable)
		if uint32(result) > smallMax {
			t.Errorf("calculateLocalPayload result %d exceeds maxLocal %d", result, smallMax)
		}
	})
}

// TestCellOverflowPrepareCellDataComposite exercises prepareCellData when CompositePK is true
// and the payload is large enough to require overflow pages.
func TestCellOverflowPrepareCellDataComposite(t *testing.T) {
	t.Parallel()

	t.Run("composite pk with overflow payload", func(t *testing.T) {
		bt := NewBtree(4096)
		root, err := bt.CreateWithoutRowidTable()
		if err != nil {
			t.Fatalf("CreateWithoutRowidTable: %v", err)
		}
		cursor := NewCursorWithOptions(bt, root, true)

		// Payload size well above the overflow threshold for 4096-byte pages
		largePayload := make([]byte, 5000)
		for i := range largePayload {
			largePayload[i] = byte(i % 251)
		}

		key := withoutrowid.EncodeCompositeKey([]interface{}{"pk-part1", int64(42)})

		// InsertWithComposite calls prepareCellData internally with CompositePK=true
		if err := cursor.InsertWithComposite(0, key, largePayload); err != nil {
			t.Fatalf("InsertWithComposite: %v", err)
		}

		// Seek back and verify payload via overflow read
		found, err := cursor.SeekComposite(key)
		if err != nil {
			t.Fatalf("SeekComposite: %v", err)
		}
		if !found {
			t.Fatal("row not found after InsertWithComposite")
		}

		if cursor.CurrentCell == nil {
			t.Fatal("CurrentCell is nil")
		}
		if cursor.CurrentCell.OverflowPage == 0 {
			t.Error("expected overflow pages for 5000-byte payload on 4096-byte pages")
		}

		complete, err := cursor.GetCompletePayload()
		if err != nil {
			t.Fatalf("GetCompletePayload: %v", err)
		}
		if !bytes.Equal(complete, largePayload) {
			t.Errorf("payload mismatch: got %d bytes, want %d bytes", len(complete), len(largePayload))
		}
	})

	t.Run("non-composite with overflow payload exercises prepareCellData int key path", func(t *testing.T) {
		bt := NewBtree(4096)
		rootPage, err := bt.CreateTable()
		if err != nil {
			t.Fatalf("CreateTable: %v", err)
		}
		cursor := NewCursor(bt, rootPage)

		// 5000 bytes triggers overflow on 4096-byte pages
		largePayload := make([]byte, 5000)
		for i := range largePayload {
			largePayload[i] = byte(i % 253)
		}

		if err := cursor.Insert(1, largePayload); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		found, err := cursor.SeekRowid(1)
		if err != nil {
			t.Fatalf("SeekRowid: %v", err)
		}
		if !found {
			t.Fatal("row not found")
		}
		if cursor.CurrentCell.OverflowPage == 0 {
			t.Error("expected overflow for 5000-byte payload")
		}

		complete, err := cursor.GetCompletePayload()
		if err != nil {
			t.Fatalf("GetCompletePayload: %v", err)
		}
		if !bytes.Equal(complete, largePayload) {
			t.Errorf("payload mismatch: got %d bytes, want %d bytes", len(complete), len(largePayload))
		}
	})
}

// TestCellOverflowIndexLargeKeys exercises extractIndexPayloadAndOverflow and
// calculateLocalPayload via raw cell construction with 200+ byte payloads.
// The IndexCursor does not support splits, so we use ParseCell directly and
// only insert a small number of entries via InsertIndex to avoid page-full errors.
func TestCellOverflowIndexLargeKeys(t *testing.T) {
	t.Parallel()

	t.Run("direct parse of 200-byte index leaf cells", func(t *testing.T) {
		// Build and parse 200-byte index leaf cells directly without inserting into a cursor.
		// This exercises parseIndexLeafCell -> completeIndexCellParse -> extractIndexPayloadAndOverflow.
		for i := 0; i < 20; i++ {
			key := []byte(fmt.Sprintf("%-200s", fmt.Sprintf("large-index-key-%05d", i)))
			// Encode a rowid varint appended to the key to form the payload
			rowidBuf := make([]byte, 9)
			n := PutVarint(rowidBuf, uint64(i+1))
			payload := append(key, rowidBuf[:n]...)
			cell := EncodeIndexLeafCell(payload)
			info, err := ParseCell(PageTypeLeafIndex, cell, 4096)
			if err != nil {
				t.Fatalf("ParseCell[%d] error = %v", i, err)
			}
			if info.PayloadSize != uint32(len(payload)) {
				t.Errorf("[%d] PayloadSize = %d, want %d", i, info.PayloadSize, len(payload))
			}
		}
	})

	t.Run("insert small number of 200-byte key entries via index cursor", func(t *testing.T) {
		// Insert just a few entries so the page doesn't overflow.
		bt := NewBtree(4096)
		rootPage, err := createIndexPage(bt)
		if err != nil {
			t.Fatalf("createIndexPage: %v", err)
		}
		cursor := NewIndexCursor(bt, rootPage)

		// 4096-byte page: maxLocal = 4061, each 200-byte key + rowid varint = ~201 bytes.
		// A 4096-byte page holds roughly 19 entries. Use 10 to stay safe.
		const n = 10
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("%-200s", fmt.Sprintf("large-index-key-%05d", i)))
			if err := cursor.InsertIndex(key, int64(i)); err != nil {
				t.Fatalf("InsertIndex(%d): %v", i, err)
			}
		}

		count := countIndexForward(cursor)
		if count != n {
			t.Errorf("expected %d entries, got %d", n, count)
		}
	})

	t.Run("direct parse of overflow index leaf cells triggers overflow branch", func(t *testing.T) {
		// We need payload > maxLocal = 4096-35 = 4061 to trigger overflow branch.
		usableSize := uint32(4096)
		maxLocal := calculateMaxLocal(usableSize, false)

		for i := 0; i < 5; i++ {
			totalPayloadSize := uint32(maxLocal) + 100 + uint32(i)*50
			localPayload := CalculateLocalPayload(totalPayloadSize, usableSize, false)

			// Determine varint size for totalPayloadSize
			varBuf := make([]byte, 9)
			varN := PutVarint(varBuf, uint64(totalPayloadSize))

			// Allocate buffer: varint + localPayload bytes + 4-byte overflow page ptr
			cellBuf := make([]byte, varN+int(localPayload)+4)
			copy(cellBuf, varBuf[:varN])
			// local payload bytes already zero-initialized
			// write overflow page number at offset varN+localPayload
			cellBuf[varN+int(localPayload)+3] = byte(i + 2)

			info, err := ParseCell(PageTypeLeafIndex, cellBuf, usableSize)
			if err != nil {
				t.Fatalf("[%d] ParseCell error = %v", i, err)
			}
			if info.OverflowPage != uint32(i+2) {
				t.Errorf("[%d] OverflowPage = %d, want %d", i, info.OverflowPage, i+2)
			}
			if info.PayloadSize != totalPayloadSize {
				t.Errorf("[%d] PayloadSize = %d, want %d", i, info.PayloadSize, totalPayloadSize)
			}
		}
	})

	t.Run("direct parseIndexLeafCell with overflow", func(t *testing.T) {
		usableSize := uint32(4096)
		maxLocal := calculateMaxLocal(usableSize, false)
		localPayload := CalculateLocalPayload(uint32(maxLocal)+100, usableSize, false)

		// Build a raw leaf index cell with overflow
		payloadSize := uint64(maxLocal) + 100
		buf := make([]byte, 9+int(localPayload)+4)
		n := PutVarint(buf, payloadSize)
		for i := 0; i < int(localPayload); i++ {
			buf[n+i] = byte(i % 127)
		}
		// Overflow page = 33
		buf[n+int(localPayload)] = 0
		buf[n+int(localPayload)+1] = 0
		buf[n+int(localPayload)+2] = 0
		buf[n+int(localPayload)+3] = 33

		info, err := ParseCell(PageTypeLeafIndex, buf[:n+int(localPayload)+4], usableSize)
		if err != nil {
			t.Fatalf("ParseCell(PageTypeLeafIndex) error = %v", err)
		}
		if info.OverflowPage != 33 {
			t.Errorf("OverflowPage = %d, want 33", info.OverflowPage)
		}
		if uint32(info.LocalPayload) != uint32(localPayload) {
			t.Errorf("LocalPayload = %d, want %d", info.LocalPayload, localPayload)
		}
	})
}
