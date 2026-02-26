package vdbe

import (
	"encoding/binary"
	"fmt"
	"math"
)

// decodeRecord decodes a SQLite record back to a slice of values
func decodeRecord(data []byte) ([]interface{}, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty record")
	}

	// Read header size
	headerSize, n := getVarint(data, 0)
	if n == 0 {
		return nil, fmt.Errorf("invalid header size")
	}

	offset := n

	// Read serial types from header
	serialTypes := make([]uint64, 0)
	for offset < int(headerSize) {
		st, n := getVarint(data, offset)
		if n == 0 {
			return nil, fmt.Errorf("invalid serial type at offset %d", offset)
		}
		serialTypes = append(serialTypes, st)
		offset += n
	}

	// Read values from body
	values := make([]interface{}, len(serialTypes))
	for i, st := range serialTypes {
		val, n, err := decodeValue(data, offset, st)
		if err != nil {
			return nil, fmt.Errorf("failed to decode value %d: %w", i, err)
		}
		values[i] = val
		offset += n
	}

	return values, nil
}

// decodeZeroWidthConst maps serial types with no stored data to their Go values.
var decodeZeroWidthConst = map[uint64]interface{}{
	0: nil,       // NULL
	8: int64(0),  // integer constant 0
	9: int64(1),  // integer constant 1
}

// decodeIntWidth maps serial type 1–6 to their byte widths.
var decodeIntWidth = [7]int{0, 1, 2, 3, 4, 6, 8} // index 0 unused

// decodeFixedInt reads a big-endian signed integer of the width dictated by
// serial type st (1–6) from data at offset.
func decodeFixedInt(data []byte, offset int, st uint64) (interface{}, int, error) {
	width := decodeIntWidth[st]
	if offset+width > len(data) {
		return nil, 0, fmt.Errorf("truncated int%d", width*8)
	}
	v := decodeIntValue(data, offset, st)
	return v, width, nil
}

// decodeIntValue extracts the integer value based on serial type.
func decodeIntValue(data []byte, offset int, st uint64) int64 {
	switch st {
	case 1:
		return int64(int8(data[offset]))
	case 2:
		return int64(int16(binary.BigEndian.Uint16(data[offset:])))
	case 3:
		return decodeInt24Value(data, offset)
	case 4:
		return int64(int32(binary.BigEndian.Uint32(data[offset:])))
	case 5:
		return decodeInt48Value(data, offset)
	default:
		return int64(binary.BigEndian.Uint64(data[offset:]))
	}
}

// decodeInt24Value decodes a 24-bit signed integer.
func decodeInt24Value(data []byte, offset int) int64 {
	v := int32(data[offset])<<16 | int32(data[offset+1])<<8 | int32(data[offset+2])
	if v&0x800000 != 0 {
		v |= ^0xffffff
	}
	return int64(v)
}

// decodeInt48Value decodes a 48-bit signed integer.
func decodeInt48Value(data []byte, offset int) int64 {
	v := int64(data[offset])<<40 | int64(data[offset+1])<<32 |
		int64(data[offset+2])<<24 | int64(data[offset+3])<<16 |
		int64(data[offset+4])<<8 | int64(data[offset+5])
	if v&0x800000000000 != 0 {
		v |= ^0xffffffffffff
	}
	return v
}

// decodeFloat64 reads an IEEE 754 float64 from data at offset.
func decodeFloat64(data []byte, offset int) (interface{}, int, error) {
	if offset+8 > len(data) {
		return nil, 0, fmt.Errorf("truncated float64")
	}
	bits := binary.BigEndian.Uint64(data[offset:])
	return math.Float64frombits(bits), 8, nil
}

// decodeBlobOrText reads a blob (even serial type) or text (odd serial type)
// from data at offset.
func decodeBlobOrText(data []byte, offset int, serialType uint64) (interface{}, int, error) {
	length := serialTypeLen(serialType)
	if offset+length > len(data) {
		return nil, 0, fmt.Errorf("truncated blob/text")
	}
	b := make([]byte, length)
	copy(b, data[offset:offset+length])
	if serialType%2 == 0 {
		return b, length, nil // BLOB
	}
	return string(b), length, nil // TEXT
}

// decodeValue decodes a single value from the record body.
func decodeValue(data []byte, offset int, serialType uint64) (interface{}, int, error) {
	if v, ok := decodeZeroWidthConst[serialType]; ok {
		return v, 0, nil
	}
	if serialType >= 1 && serialType <= 6 {
		return decodeFixedInt(data, offset, serialType)
	}
	if serialType == 7 {
		return decodeFloat64(data, offset)
	}
	return decodeBlobOrText(data, offset, serialType)
}
