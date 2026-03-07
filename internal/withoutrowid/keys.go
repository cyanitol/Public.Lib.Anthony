// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
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
	return string(bytes.ReplaceAll([]byte(fmt.Sprintf("%v", v)), []byte{0x00}, []byte{0x01}))
}
