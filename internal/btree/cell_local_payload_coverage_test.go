// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"testing"
)

// TestCellLocalPayloadCalculateLocalPayload exercises all branches of
// calculateLocalPayload, including the defensive error paths that require
// minLocal or surplus to exceed math.MaxUint16.
func TestCellLocalPayloadCalculateLocalPayload_EdgeCases(t *testing.T) {
	t.Parallel()
	// usableSize < 4
	if result := calculateLocalPayload(1000, 50, 900, 3); result != 50 {
		t.Errorf("usableSize=3: got %d, want 50", result)
	}
	if result := calculateLocalPayload(100, 0, 10, 2); result != 0 {
		t.Errorf("usableSize=2 minLocal=0: got %d, want 0", result)
	}
	// payloadSize < minLocal
	if result := calculateLocalPayload(5, 10, 50, 512); result != 10 {
		t.Errorf("payloadSize<minLocal: got %d, want 10", result)
	}
}

func TestCellLocalPayloadCalculateLocalPayload_Surplus(t *testing.T) {
	t.Parallel()
	usableSize := uint32(512)
	maxLocal := calculateMaxLocal(usableSize, true)
	minLocal := calculateMinLocal(usableSize, true)
	payloadSize := minLocal + 1
	surplus := minLocal + (payloadSize-minLocal)%(usableSize-4)
	result := calculateLocalPayload(payloadSize, minLocal, maxLocal, usableSize)
	if surplus <= maxLocal && uint32(result) != surplus {
		t.Errorf("surplus<=maxLocal: got %d, want surplus %d", result, surplus)
	}
	// surplus > maxLocal
	excess := maxLocal - minLocal + 1
	payloadSize = minLocal + excess
	result = calculateLocalPayload(payloadSize, minLocal, maxLocal, usableSize)
	if uint32(result) != minLocal {
		t.Errorf("surplus>maxLocal: got %d, want minLocal %d", result, minLocal)
	}
}

func TestCellLocalPayloadCalculateLocalPayload_Overflow(t *testing.T) {
	t.Parallel()
	// large minLocal overflows uint16
	if result := calculateLocalPayload(1000, 70000, 71000, 3); result != 0 {
		t.Errorf("usableSize<4 with oversized minLocal: got %d, want 0", result)
	}
	// surplus > maxLocal with large minLocal
	result := calculateLocalPayload(70000+10001, 70000, 80000, 100000)
	if result != 0 {
		t.Errorf("Path3 oversized minLocal: got %d, want 0", result)
	}
	// surplus <= maxLocal but surplus > uint16
	result = calculateLocalPayload(70000, 70000, 90000, 100000)
	if result != 0 {
		t.Errorf("Path2 oversized surplus+minLocal: got %d, want 0", result)
	}
}

// TestCellLocalPayloadCalculateMinLocal exercises all branches of calculateMinLocal,
// including the intermediate < 23 branch that requires usableSize in [35, 195).
func TestCellLocalPayloadCalculateMinLocal_SmallSizes(t *testing.T) {
	t.Parallel()
	for _, us := range []uint32{0, 34, 100, 195, 196} {
		result := calculateMinLocal(us, true)
		if result != 0 {
			t.Errorf("usableSize=%d: got %d, want 0", us, result)
		}
	}
}

func TestCellLocalPayloadCalculateMinLocal_LargeSize(t *testing.T) {
	t.Parallel()
	result := calculateMinLocal(4096, true)
	if result == 0 {
		t.Errorf("usableSize=4096: got 0, want positive minLocal")
	}
	maxLocal := calculateMaxLocal(4096, true)
	if result > maxLocal {
		t.Errorf("usableSize=4096: minLocal=%d exceeds maxLocal=%d", result, maxLocal)
	}
	// isTable should not affect result
	r2 := calculateMinLocal(4096, false)
	if result != r2 {
		t.Errorf("isTable should not affect result: true=%d false=%d", result, r2)
	}
}

// TestCellLocalPayloadParseTableLeafCellEmpty covers the empty cell data error path
// in parseTableLeafCell (line 47-49 of cell.go).
func TestCellLocalPayloadParseTableLeafCellEmpty(t *testing.T) {
	t.Parallel()

	_, err := ParseCell(PageTypeLeafTable, []byte{}, 4096)
	if err == nil {
		t.Error("ParseCell(PageTypeLeafTable, empty) should return error, got nil")
	}
}

// TestCellLocalPayloadParseTableInteriorCellVarintErrors covers the rowid varint
// error paths in parseTableInteriorCell (lines 198-203 of cell.go).
func TestCellLocalPayloadParseTableInteriorCellVarintErrors(t *testing.T) {
	t.Parallel()

	t.Run("exactly 4 bytes gives no varint data fails rowid read", func(t *testing.T) {
		// 4 bytes = child page only, no varint bytes after → GetVarint returns n=0
		data := []byte{0x00, 0x00, 0x00, 0x01}
		_, err := ParseCell(PageTypeInteriorTable, data, 4096)
		if err == nil {
			t.Error("ParseCell(PageTypeInteriorTable, 4 bytes) should fail for missing rowid varint")
		}
	})

	t.Run("rowid varint value exceeds MaxInt64", func(t *testing.T) {
		// Build a cell: 4-byte child page + 9-byte max varint (math.MaxUint64 > MaxInt64)
		buf := make([]byte, 4+9)
		buf[0] = 0x00
		buf[1] = 0x00
		buf[2] = 0x00
		buf[3] = 0x02
		// Encode math.MaxUint64 as a 9-byte varint; all bytes 0xFF gives MaxUint64
		for i := 4; i < 13; i++ {
			buf[i] = 0xFF
		}
		_, err := ParseCell(PageTypeInteriorTable, buf, 4096)
		if err == nil {
			t.Error("ParseCell(PageTypeInteriorTable) with rowid > MaxInt64 should return error")
		}
	})
}
