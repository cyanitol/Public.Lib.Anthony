// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package utf

import "testing"

// TestMCDC_isCharRange exercises the compound boolean condition inside
// isCharRange, which is called by parseClassEntry while parsing a '[…]'
// character-class inside a GLOB pattern:
//
//	func (g *globState) isCharRange() bool {
//	    return g.pi < len(g.pattern) && g.pattern[g.pi] == '-' && g.pi+1 < len(g.pattern)
//	}
//
// Sub-conditions (evaluated left-to-right with short-circuit &&):
//
//	A = g.pi < len(g.pattern)         (position is still inside the pattern)
//	B = g.pattern[g.pi] == '-'        (current byte is the range separator '-')
//	C = g.pi+1 < len(g.pattern)       (there is at least one character after '-')
//
// Because the three clauses are joined with &&, MC/DC requires:
//   - One row where all are true → result true  (all conditions matter).
//   - One row per sub-condition where that sub-condition is the unique false
//     clause that makes the overall result false.
//
// That gives N+1 = 4 rows.
//
// isCharRange is unexported, so the tests drive it through Glob, which calls
// globImpl → matchCharClass → parseCharClass → parseClassEntry → isCharRange.
// We craft GLOB patterns and subject strings so that each row exercises the
// desired combination of sub-condition truth values.
//
// Pattern anatomy used in the tests:
//
//	[<chars>]  — standard character class
//	[a-z]      — isCharRange returns true  (range check performed)
//	[a-]       — '-' is last char → C is false (no char after '-')
//	[a!z]      — '!' is not '-' → B is false (not a range separator)
//	             Note: the class is consumed until ']', so this matches
//	             'a', '!', or 'z' as individual chars.
func TestMCDC_isCharRange(t *testing.T) {
	tests := []struct {
		// MC/DC row label documents the sub-condition truth values and the
		// scenario being exercised.
		name    string
		pattern string
		str     string
		want    bool
	}{
		// -----------------------------------------------------------------------
		// Row 1 — A=T B=T C=T → isCharRange = true (range match performed)
		// Pattern "[a-z]" matched against "m".
		//   After '[' is consumed, parseClassEntry reads 'a', pi advances.
		//   isCharRange checks:
		//     A: pi (points at '-') < len("[a-z]" minus prefix) → true
		//     B: pattern[pi] == '-'                              → true
		//     C: pi+1 < len(pattern)  (']' follows '-')         → true
		//   matchRange is called; 'a' ≤ 'm' ≤ 'z' → range match → Glob = true.
		// -----------------------------------------------------------------------
		{
			name:    "A=T B=T C=T: range [a-z] matches 'm' → Glob true",
			pattern: "[a-z]",
			str:     "m",
			want:    true,
		},

		// -----------------------------------------------------------------------
		// Row 2 — A=T B=T C=T → isCharRange = true (range does NOT match char)
		// Pattern "[a-z]" matched against "M" (uppercase, outside range).
		//   Same path as Row 1 but matchRange returns false → Glob = false.
		//   Included to confirm that the range branch is reached and evaluated.
		// -----------------------------------------------------------------------
		{
			name:    "A=T B=T C=T: range [a-z] does not match 'M' → Glob false",
			pattern: "[a-z]",
			str:     "M",
			want:    false,
		},

		// -----------------------------------------------------------------------
		// Row 3 — A=T B=T C=F → isCharRange = false (B true but C false)
		// Pattern "[a-]" matched against "a".
		//   After '[' and 'a' are consumed, pi points at '-'.
		//     A: pi < len(pattern)         → true  (T)
		//     B: pattern[pi] == '-'        → true  (T)
		//     C: pi+1 < len(pattern)? The only char after '-' is ']', so
		//        pi+1 IS within bounds but ']' would end the class; however
		//        the condition only checks the index bound, not the value.
		//        To make C strictly false we need '-' to be the very last
		//        byte of the pattern string with no trailing ']'.
		//        With pattern "[a-": after '[' (psize=1) is consumed inside
		//        matchCharClass, pi points at 'a'; parseClassEntry reads 'a'
		//        (pi now at '-'); isCharRange:
		//          A: pi(=2) < len("[a-")(=3) → true
		//          B: pattern[2] == '-'        → true
		//          C: pi+1(=3) < 3             → FALSE  ← unique false clause
		//        → isCharRange returns false; 'a' is treated as literal.
		//        'a' == 'a' → matched=true; no ']' found → loop ends when
		//        DecodeRune returns 0 size at end-of-pattern.
		//        matchCharClass returns true → Glob returns true.
		// -----------------------------------------------------------------------
		{
			name:    "A=T B=T C=F: '-' at end of pattern [a- → no range (C flips)",
			pattern: "[a-",
			str:     "a",
			want:    true,
		},

		// -----------------------------------------------------------------------
		// Row 4 — A=T B=F C=T/F → isCharRange = false (B false is unique)
		// Pattern "[a!z]" matched against "!".
		//   After '[' is consumed, parseClassEntry reads 'a'; pi points at '!'.
		//     A: pi < len(pattern)          → true  (T)
		//     B: pattern[pi] == '-'? '!'≠'-'→ FALSE  ← unique false clause
		//        (short-circuit: C is never evaluated)
		//   → isCharRange returns false; 'a' is compared as a literal ('a'≠'!').
		//   Loop continues: reads '!'; isCharRange: A=T, B: '!'≠'-' → false;
		//   '!' == '!' → matched=true; loop continues; reads 'z' then ']' → done.
		//   matched=true → matchCharClass returns true → Glob true.
		// -----------------------------------------------------------------------
		{
			name:    "A=T B=F C=?: non-range separator [a!z] matches '!' → no range (B flips)",
			pattern: "[a!z]",
			str:     "!",
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Glob(tt.pattern, tt.str)
			if got != tt.want {
				t.Errorf("Glob(%q, %q) = %v, want %v", tt.pattern, tt.str, got, tt.want)
			}
		})
	}
}
