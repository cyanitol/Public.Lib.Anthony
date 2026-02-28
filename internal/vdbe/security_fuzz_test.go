package vdbe

import (
	"testing"
)

// FuzzDecodeRecord tests decoding random byte sequences
func FuzzDecodeRecord(f *testing.F) {
	// Add seed corpus with various record formats

	// Empty record
	f.Add([]byte{})

	// Minimal valid record: header size = 1, no columns
	f.Add([]byte{0x01})

	// Single NULL value: header size = 2, serial type 0 (NULL)
	f.Add([]byte{0x02, 0x00})

	// Single integer 0: header size = 2, serial type 8
	f.Add([]byte{0x02, 0x08})

	// Single integer 1: header size = 2, serial type 9
	f.Add([]byte{0x02, 0x09})

	// Single byte integer: header size = 2, serial type 1, value 42
	f.Add([]byte{0x02, 0x01, 0x2A})

	// Truncated records
	f.Add([]byte{0x05, 0x01}) // claims header size 5 but only 2 bytes
	f.Add([]byte{0x02, 0x01}) // claims int8 but no data

	// Invalid serial types
	f.Add([]byte{0x02, 0xFF})

	// Random bytes
	f.Add(make([]byte, 1000))

	// Malformed varints
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	// Fuzz function
	f.Fuzz(func(t *testing.T, data []byte) {
		// Should not panic on any input
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("decodeRecord panicked on input: %x\nPanic: %v", data, r)
			}
		}()

		// We don't care if it returns an error, just that it doesn't panic
		_, _ = decodeRecord(data)
	})
}

// FuzzGetVarint tests the varint decoder
func FuzzGetVarint(f *testing.F) {
	// Valid varints
	f.Add([]byte{0x00}, 0)                      // 0
	f.Add([]byte{0x7F}, 0)                      // 127
	f.Add([]byte{0x81, 0x00}, 0)                // 128
	f.Add([]byte{0xFF, 0x7F}, 0)                // max 2-byte
	f.Add([]byte{0x81, 0x80, 0x00}, 0)          // 3-byte
	f.Add([]byte{0x81, 0x80, 0x80, 0x00}, 0)    // 4-byte
	f.Add([]byte{0x81, 0x80, 0x80, 0x80, 0x00}, 0) // 5-byte

	// Truncated varints
	f.Add([]byte{0x81}, 0)          // incomplete
	f.Add([]byte{0x81, 0x80}, 0)    // incomplete
	f.Add([]byte{}, 0)              // empty

	// Maximum length varint
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}, 0)

	// Varint at different offsets
	f.Add([]byte{0x00, 0x00, 0x7F}, 2)
	f.Add([]byte{0xFF, 0xFF, 0x81, 0x00}, 2)

	f.Fuzz(func(t *testing.T, data []byte, offset int) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("getVarint panicked on data=%x offset=%d\nPanic: %v", data, offset, r)
			}
		}()

		// Should not panic
		_, _ = getVarint(data, offset)
	})
}

// FuzzDecodeValue tests individual value decoding
func FuzzDecodeValue(f *testing.F) {
	// Various serial types with data
	f.Add([]byte{0x00}, 0, uint64(0))  // NULL
	f.Add([]byte{0x00}, 0, uint64(8))  // int 0
	f.Add([]byte{0x00}, 0, uint64(9))  // int 1
	f.Add([]byte{0x2A}, 0, uint64(1))  // int8
	f.Add([]byte{0x03, 0xE8}, 0, uint64(2)) // int16

	// Blob
	f.Add([]byte{'t', 'e', 's', 't'}, 0, uint64(16)) // 4-byte blob (12 + 4)

	// String
	f.Add([]byte{'h', 'i'}, 0, uint64(17)) // 2-byte string (13 + 4)

	// Truncated data
	f.Add([]byte{}, 0, uint64(1))  // claims int8 but no data
	f.Add([]byte{0x00}, 0, uint64(2)) // claims int16 but only 1 byte

	f.Fuzz(func(t *testing.T, data []byte, offset int, serialType uint64) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("decodeValue panicked on data=%x offset=%d st=%d\nPanic: %v",
					data, offset, serialType, r)
			}
		}()

		// Should not panic
		_, _, _ = decodeValue(data, offset, serialType)
	})
}

// TestFuzzCorpusRegression ensures previously found crashes don't recur
func TestFuzzCorpusRegression(t *testing.T) {
	regressionInputs := [][]byte{
		{},
		{0x00},
		{0xFF, 0xFF, 0xFF},
		{0x02, 0xFF}, // invalid serial type
		{0x05, 0x01}, // truncated
	}

	for i, input := range regressionInputs {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Panic on regression input %d: %v\nInput: %x", i, r, input)
				}
			}()

			_, _ = decodeRecord(input)
		})
	}
}
