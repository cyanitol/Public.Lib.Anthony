// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package utf

import "testing"

// TestMCDC_isInvalidUTF8 exercises the compound boolean condition inside
// isInvalidUTF8, which is called by decodeMultiByte after assembling the
// decoded code-point value c:
//
//	func isInvalidUTF8(c uint32) bool {
//	    return c < 0x80 || (c&0xFFFFF800) == 0xD800 || (c&0xFFFFFFFE) == 0xFFFE
//	}
//
// Sub-conditions:
//
//	A = c < 0x80                      (ASCII-range after multi-byte decode → always invalid)
//	B = (c & 0xFFFFF800) == 0xD800    (UTF-16 surrogate range 0xD800–0xDFFF)
//	C = (c & 0xFFFFFFFE) == 0xFFFE    (non-character 0xFFFE or 0xFFFF)
//
// Because the three clauses are joined with ||, MC/DC requires one test case
// where the overall result is false (all three sub-conditions false) plus one
// case per sub-condition where that sub-condition is the unique one that flips
// the overall outcome (i.e. it is the only true clause).  That gives N+1 = 4
// rows.
//
// Because isInvalidUTF8 is unexported the tests drive it through DecodeRune,
// which calls decodeMultiByte → isInvalidUTF8.  We craft raw byte sequences
// that cause decodeMultiByte to assemble specific decoded values so that each
// desired sub-condition is active.
//
// Encoding scheme used below (two-byte sequences):
//
//	first byte  = 0xC0 | (c >> 6)
//	second byte = 0x80 | (c & 0x3F)
//
// (Note: DecodeRune/decodeMultiByte tolerates over-long two-byte encodings
// when testing internal paths; the resulting c value is what matters for
// isInvalidUTF8.)
func TestMCDC_isInvalidUTF8(t *testing.T) {
	// Helper: build a raw two-byte sequence whose decodeMultiByte output is c.
	// utf8Trans1 maps first_byte-0xC0 → initial accumulator value.
	// For c in 0x40..0x7F: first_byte = 0xC0|(c>>6) = 0xC1, table gives 0x01,
	// then c = (0x01<<6) | (second & 0x3F) = 0x40 | low6.
	twoByteFor := func(c uint32) []byte {
		return []byte{byte(0xC0 | (c >> 6)), byte(0x80 | (c & 0x3F))}
	}

	// Build a four-byte sequence for large code-points (surrogates, 0xFFFE, etc.)
	// using the standard UTF-8 multi-byte encoding that decodeMultiByte follows.
	// For a value c that needs 3 continuation bytes:
	//   first_byte in 0xF0..0xF7 → utf8Trans1 gives (c>>18)&0x07
	//   then each continuation adds 6 bits.
	fourByteFor := func(c uint32) []byte {
		// We use the same formula as AppendRune for 4-byte sequences.
		return []byte{
			byte(0xF0 | (c >> 18)),
			byte(0x80 | ((c >> 12) & 0x3F)),
			byte(0x80 | ((c >> 6) & 0x3F)),
			byte(0x80 | (c & 0x3F)),
		}
	}

	threeByteFor := func(c uint32) []byte {
		return []byte{
			byte(0xE0 | (c >> 12)),
			byte(0x80 | ((c >> 6) & 0x3F)),
			byte(0x80 | (c & 0x3F)),
		}
	}

	_ = fourByteFor // suppress unused warning if not used below

	tests := []struct {
		// MC/DC row label documents which sub-conditions are true/false and
		// the expected outcome of isInvalidUTF8 (true → DecodeRune returns
		// ReplacementChar; false → DecodeRune returns the actual rune).
		name        string
		input       []byte
		wantInvalid bool // true means DecodeRune should return ReplacementChar
	}{
		// -----------------------------------------------------------------------
		// Row 1 — A=F B=F C=F → isInvalidUTF8 = false (baseline: valid rune)
		// c = 0x0100 (U+0100 LATIN CAPITAL LETTER A WITH MACRON)
		//   A: 0x0100 < 0x80?                         NO  (F)
		//   B: (0x0100 & 0xFFFFF800) == 0x0000 ≠ 0xD800  NO  (F)
		//   C: (0x0100 & 0xFFFFFFFE) == 0x0100 ≠ 0xFFFE  NO  (F)
		// Outcome: false → valid rune returned.
		// -----------------------------------------------------------------------
		{
			name:        "A=F B=F C=F: valid rune 0x0100 → not invalid",
			input:       twoByteFor(0x0100),
			wantInvalid: false,
		},

		// -----------------------------------------------------------------------
		// Row 2 — A=T B=F C=F → isInvalidUTF8 = true (A alone flips outcome)
		// c = 0x41 ('A'), assembled via an over-long two-byte encoding.
		//   A: 0x41 < 0x80?                           YES (T) ← unique true clause
		//   B: (0x41 & 0xFFFFF800) == 0x0000 ≠ 0xD800 NO  (F)
		//   C: (0x41 & 0xFFFFFFFE) == 0x0040 ≠ 0xFFFE NO  (F)
		// Outcome: true → ReplacementChar returned.
		// -----------------------------------------------------------------------
		{
			name:        "A=T B=F C=F: over-long ASCII 0x41 → invalid (A flips)",
			input:       twoByteFor(0x41),
			wantInvalid: true,
		},

		// -----------------------------------------------------------------------
		// Row 3 — A=F B=T C=F → isInvalidUTF8 = true (B alone flips outcome)
		// c = 0xD800 (first UTF-16 high surrogate).
		//   A: 0xD800 < 0x80?                           NO  (F)
		//   B: (0xD800 & 0xFFFFF800) == 0xD800?          YES (T) ← unique true clause
		//   C: (0xD800 & 0xFFFFFFFE) == 0xD800 ≠ 0xFFFE NO  (F)
		// Outcome: true → ReplacementChar returned.
		// -----------------------------------------------------------------------
		{
			name:        "A=F B=T C=F: surrogate 0xD800 → invalid (B flips)",
			input:       threeByteFor(0xD800),
			wantInvalid: true,
		},

		// -----------------------------------------------------------------------
		// Row 4 — A=F B=F C=T → isInvalidUTF8 = true (C alone flips outcome)
		// c = 0xFFFE (Unicode non-character).
		//   A: 0xFFFE < 0x80?                            NO  (F)
		//   B: (0xFFFE & 0xFFFFF800) == 0xF800 ≠ 0xD800  NO  (F)
		//   C: (0xFFFE & 0xFFFFFFFE) == 0xFFFE?           YES (T) ← unique true clause
		// Outcome: true → ReplacementChar returned.
		// -----------------------------------------------------------------------
		{
			name:        "A=F B=F C=T: non-character 0xFFFE → invalid (C flips)",
			input:       threeByteFor(0xFFFE),
			wantInvalid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _ := DecodeRune(tt.input)
			gotInvalid := r == ReplacementChar
			if gotInvalid != tt.wantInvalid {
				t.Errorf("DecodeRune(%#x) returned rune %U (invalid=%v), want invalid=%v",
					tt.input, r, gotInvalid, tt.wantInvalid)
			}
		})
	}
}
