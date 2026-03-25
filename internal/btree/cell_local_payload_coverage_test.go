// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package btree

import (
	"testing"
)

// TestCellLocalPayloadCalculateLocalPayload exercises all branches of
// calculateLocalPayload, including the defensive error paths that require
// minLocal or surplus to exceed math.MaxUint16.
func TestCellLocalPayloadCalculateLocalPayload(t *testing.T) {
	t.Parallel()

	t.Run("usableSize less than 4 returns minLocal", func(t *testing.T) {
		// usableSize < 4 → early return SafeCastUint32ToUint16(minLocal)
		result := calculateLocalPayload(1000, 50, 900, 3)
		if result != 50 {
			t.Errorf("usableSize=3: got %d, want 50", result)
		}
	})

	t.Run("usableSize less than 4 with zero minLocal returns zero", func(t *testing.T) {
		result := calculateLocalPayload(100, 0, 10, 2)
		if result != 0 {
			t.Errorf("usableSize=2 minLocal=0: got %d, want 0", result)
		}
	})

	t.Run("payloadSize less than minLocal returns minLocal", func(t *testing.T) {
		// payloadSize < minLocal triggers first OR branch (second condition)
		result := calculateLocalPayload(5, 10, 50, 512)
		if result != 10 {
			t.Errorf("payloadSize<minLocal: got %d, want 10", result)
		}
	})

	t.Run("surplus less than or equal to maxLocal returns surplus", func(t *testing.T) {
		// usableSize=512: maxLocal=477, minLocal=(500*32/255)-23=39 (approx)
		usableSize := uint32(512)
		maxLocal := calculateMaxLocal(usableSize, true)
		minLocal := calculateMinLocal(usableSize, true)
		// payloadSize = minLocal+1 → surplus = minLocal + 1%(usableSize-4) = minLocal+1
		payloadSize := minLocal + 1
		surplus := minLocal + (payloadSize-minLocal)%(usableSize-4)
		result := calculateLocalPayload(payloadSize, minLocal, maxLocal, usableSize)
		if surplus <= maxLocal && uint32(result) != surplus {
			t.Errorf("surplus<=maxLocal: got %d, want surplus %d", result, surplus)
		}
	})

	t.Run("surplus greater than maxLocal returns minLocal", func(t *testing.T) {
		// Choose payloadSize so surplus = minLocal + excess > maxLocal
		usableSize := uint32(512)
		maxLocal := calculateMaxLocal(usableSize, true)
		minLocal := calculateMinLocal(usableSize, true)
		// excess = maxLocal - minLocal + 1 makes surplus = maxLocal + 1
		excess := maxLocal - minLocal + 1
		payloadSize := minLocal + excess
		result := calculateLocalPayload(payloadSize, minLocal, maxLocal, usableSize)
		if uint32(result) != minLocal {
			t.Errorf("surplus>maxLocal: got %d, want minLocal %d", result, minLocal)
		}
	})

	t.Run("usableSize less than 4 with large minLocal overflows uint16 returns zero", func(t *testing.T) {
		// minLocal > math.MaxUint16 causes SafeCastUint32ToUint16 to fail → return 0
		// This exercises the err != nil path in the first branch of calculateLocalPayload.
		const largeMinLocal = uint32(70000) // > 65535
		result := calculateLocalPayload(1000, largeMinLocal, largeMinLocal+1000, 3)
		if result != 0 {
			t.Errorf("usableSize<4 with oversized minLocal: got %d, want 0", result)
		}
	})

	t.Run("surplus exceeds maxLocal with large minLocal overflows uint16 returns zero", func(t *testing.T) {
		// Path 3: surplus > maxLocal, then SafeCastUint32ToUint16(minLocal) fails → return 0
		// Need minLocal > 65535, surplus > maxLocal, payloadSize >= minLocal, usableSize >= 4.
		const largeMinLocal = uint32(70000)
		const largeMaxLocal = uint32(80000)
		// surplus = largeMinLocal + (payloadSize-largeMinLocal)%(usableSize-4)
		// Choose payloadSize so surplus > largeMaxLocal
		usableSize := uint32(100000) // large to avoid usableSize<4
		// surplus = largeMinLocal + excess where excess = largeMaxLocal - largeMinLocal + 1
		excess := largeMaxLocal - largeMinLocal + 1
		payloadSize := largeMinLocal + excess
		result := calculateLocalPayload(payloadSize, largeMinLocal, largeMaxLocal, usableSize)
		// surplus = largeMinLocal + excess = largeMaxLocal + 1 > largeMaxLocal → Path 3
		// SafeCastUint32ToUint16(largeMinLocal) fails → return 0
		if result != 0 {
			t.Errorf("Path3 oversized minLocal: got %d, want 0", result)
		}
	})

	t.Run("surplus fits in uint16 but inner fallback to minLocal also overflows returns zero", func(t *testing.T) {
		// Path 2 (surplus <= maxLocal): SafeCastUint32ToUint16(surplus) fails (surplus > 65535),
		// then inner fallback SafeCastUint32ToUint16(minLocal) also fails → return 0.
		// Need: surplus > 65535, surplus <= maxLocal, payloadSize >= minLocal, usableSize >= 4.
		const largeMinLocal = uint32(70000)
		const largeMaxLocal = uint32(90000)
		usableSize := uint32(100000)
		// Pick payloadSize = minLocal (surplus = minLocal + 0 = minLocal = 70000 > 65535, <= maxLocal)
		payloadSize := largeMinLocal
		result := calculateLocalPayload(payloadSize, largeMinLocal, largeMaxLocal, usableSize)
		// surplus = largeMinLocal = 70000 <= largeMaxLocal → Path 2
		// SafeCastUint32ToUint16(70000) fails → fallback SafeCastUint32ToUint16(70000) also fails → 0
		if result != 0 {
			t.Errorf("Path2 oversized surplus+minLocal: got %d, want 0", result)
		}
	})
}

// TestCellLocalPayloadCalculateMinLocal exercises all branches of calculateMinLocal,
// including the intermediate < 23 branch that requires usableSize in [35, 195).
func TestCellLocalPayloadCalculateMinLocal(t *testing.T) {
	t.Parallel()

	t.Run("usableSize below MinUsableSize returns zero", func(t *testing.T) {
		// usableSize < MinUsableSize (35) → return 0
		result := calculateMinLocal(34, true)
		if result != 0 {
			t.Errorf("usableSize=34: got %d, want 0", result)
		}
	})

	t.Run("usableSize of zero returns zero", func(t *testing.T) {
		result := calculateMinLocal(0, false)
		if result != 0 {
			t.Errorf("usableSize=0: got %d, want 0", result)
		}
	})

	t.Run("usableSize in range where intermediate is less than 23 returns zero", func(t *testing.T) {
		// intermediate = (usableSize-12)*32/255
		// For usableSize=100: (100-12)*32/255 = 88*32/255 = 11 < 23 → return 0
		// Range: MinUsableSize(35) <= usableSize < 195 gives intermediate < 23
		result := calculateMinLocal(100, true)
		if result != 0 {
			t.Errorf("usableSize=100 (intermediate<23): got %d, want 0", result)
		}
	})

	t.Run("usableSize just at lower boundary of intermediate equals 23", func(t *testing.T) {
		// (usableSize-12)*32/255 = 23 → usableSize-12 = 23*255/32 = 183.28 → usableSize-12 >= 184 → usableSize=196
		// usableSize=195: (195-12)*32/255 = 183*32/255 = 5856/255 = 22 < 23 → return 0
		result := calculateMinLocal(195, false)
		if result != 0 {
			t.Errorf("usableSize=195 (intermediate=22): got %d, want 0", result)
		}
	})

	t.Run("usableSize=196 gives intermediate exactly 23 returns zero", func(t *testing.T) {
		// (196-12)*32/255 = 184*32/255 = 5888/255 = 23 (integer division) → 23 - 23 = 0
		result := calculateMinLocal(196, true)
		if result != 0 {
			t.Errorf("usableSize=196 (intermediate=23): got %d, want 0", result)
		}
	})

	t.Run("large usableSize returns positive minLocal", func(t *testing.T) {
		// usableSize=4096: intermediate = (4096-12)*32/255 = 4084*32/255 = 512 → 512-23 = 489
		result := calculateMinLocal(4096, true)
		if result == 0 {
			t.Errorf("usableSize=4096: got 0, want positive minLocal")
		}
		maxLocal := calculateMaxLocal(4096, true)
		if result > maxLocal {
			t.Errorf("usableSize=4096: minLocal=%d exceeds maxLocal=%d", result, maxLocal)
		}
	})

	t.Run("isTable false gives same result as isTable true", func(t *testing.T) {
		// The isTable parameter is unused in calculateMinLocal
		r1 := calculateMinLocal(4096, true)
		r2 := calculateMinLocal(4096, false)
		if r1 != r2 {
			t.Errorf("isTable should not affect result: true=%d false=%d", r1, r2)
		}
	})
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
