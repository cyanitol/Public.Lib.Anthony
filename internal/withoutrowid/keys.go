// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
// Package withoutrowid provides helpers for encoding composite primary keys
// for WITHOUT ROWID tables. The encoding is order-preserving for SQLite
// default binary comparisons so that bytewise ordering matches comparison
// semantics across integers, reals, text, and blobs.
package withoutrowid

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

// EncodeCompositeKey encodes a composite primary key into an order-preserving
// byte slice. It supports integers, floats, text, blobs, and nil (NULL).
// Collation is assumed to be binary for now; caller must supply already
// collated text if needed.
func EncodeCompositeKey(values []interface{}) []byte {
	var buf bytes.Buffer
	for _, v := range values {
		switch val := v.(type) {
		case nil:
			// NULLs sort before all other values
			buf.WriteByte(0x00)
		case int:
			buf.WriteByte(0x10)
			buf.Write(encodeInt64(int64(val)))
		case int64:
			buf.WriteByte(0x10)
			buf.Write(encodeInt64(val))
		case float64:
			buf.WriteByte(0x20)
			buf.Write(encodeFloat64(val))
		case string:
			buf.WriteByte(0x30)
			buf.WriteString(val)
			buf.WriteByte(0x00) // terminator to avoid prefix collisions
		case []byte:
			buf.WriteByte(0x40)
			buf.Write(val)
			buf.WriteByte(0x00)
		default:
			// Fallback: format using default string representation.
			buf.WriteByte(0x50)
			buf.WriteString(formatUnknown(val))
			buf.WriteByte(0x00)
		}
	}
	return buf.Bytes()
}

// encodeInt64 returns a big-endian two's complement sortable representation.
func encodeInt64(v int64) []byte {
	var out [8]byte
	// Flip the sign bit so negative ints sort before positive but preserve order.
	u := uint64(v) ^ (1 << 63)
	binary.BigEndian.PutUint64(out[:], u)
	return out[:]
}

// encodeFloat64 returns a sortable big-endian representation.
func encodeFloat64(v float64) []byte {
	var out [8]byte
	bits := math.Float64bits(v)
	if v >= 0 {
		bits |= (1 << 63)
	} else {
		bits = ^bits
	}
	binary.BigEndian.PutUint64(out[:], bits)
	return out[:]
}

func formatUnknown(v interface{}) string {
	// Ensure no embedded NULs to avoid prefix ambiguity.
	return string(bytes.ReplaceAll([]byte(fmt.Sprintf("%v", v)), []byte{0x00}, []byte{0x01}))
}

// DecodeCompositeKey decodes a composite key produced by EncodeCompositeKey.
// It returns the slice of values in order or an error if the key bytes are malformed.
func DecodeCompositeKey(data []byte) ([]interface{}, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var values []interface{}
	for i := 0; i < len(data); {
		prefix := data[i]
		i++

		switch prefix {
		case 0x00:
			values = append(values, nil)
		case 0x10:
			if i+8 > len(data) {
				return nil, fmt.Errorf("truncated integer in composite key")
			}
			values = append(values, decodeInt64(data[i:i+8]))
			i += 8
		case 0x20:
			if i+8 > len(data) {
				return nil, fmt.Errorf("truncated float in composite key")
			}
			values = append(values, decodeFloat64(data[i:i+8]))
			i += 8
		case 0x30, 0x50: // text encodings (0x50 is fallback string)
			end := bytes.IndexByte(data[i:], 0x00)
			if end == -1 {
				return nil, fmt.Errorf("unterminated text in composite key")
			}
			values = append(values, string(data[i:i+end]))
			i += end + 1
		case 0x40: // blob
			end := bytes.IndexByte(data[i:], 0x00)
			if end == -1 {
				return nil, fmt.Errorf("unterminated blob in composite key")
			}
			values = append(values, append([]byte(nil), data[i:i+end]...))
			i += end + 1
		default:
			return nil, fmt.Errorf("unknown composite key prefix 0x%x", prefix)
		}
	}

	return values, nil
}

// decodeInt64 reverses the EncodeCompositeKey integer encoding.
func decodeInt64(b []byte) int64 {
	u := binary.BigEndian.Uint64(b)
	u ^= 1 << 63
	return int64(u)
}

// decodeFloat64 reverses the EncodeCompositeKey float encoding.
func decodeFloat64(b []byte) float64 {
	u := binary.BigEndian.Uint64(b)
	if u&(1<<63) != 0 {
		u &^= 1 << 63
		return math.Float64frombits(u)
	}
	return math.Float64frombits(^u)
}
