// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package driver

// deduplicateRowsBy removes duplicate rows, preserving order of first
// occurrence.  The caller supplies a keyFunc that serialises a row into a
// comparable string.
func deduplicateRowsBy[T any](rows [][]T, keyFunc func([]T) string) [][]T {
	seen := make(map[string]struct{}, len(rows))
	result := make([][]T, 0, len(rows))
	for _, row := range rows {
		key := keyFunc(row)
		if _, dup := seen[key]; !dup {
			seen[key] = struct{}{}
			result = append(result, row)
		}
	}
	return result
}

// intersectRowsBy returns rows that appear in both left and right
// (deduplicated), preserving the order from left.
func intersectRowsBy[T any](left, right [][]T, keyFunc func([]T) string) [][]T {
	rightSet := make(map[string]struct{}, len(right))
	for _, row := range right {
		rightSet[keyFunc(row)] = struct{}{}
	}

	seen := make(map[string]struct{}, len(left))
	result := make([][]T, 0, len(left))
	for _, row := range left {
		k := keyFunc(row)
		if _, inRight := rightSet[k]; !inRight {
			continue
		}
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		result = append(result, row)
	}
	return result
}

// filterRowsBy returns the subset of rows for which predicate returns true.
func filterRowsBy[T any](rows [][]T, predicate func([]T) bool) [][]T {
	var result [][]T
	for _, row := range rows {
		if predicate(row) {
			result = append(result, row)
		}
	}
	return result
}

// compareNulls handles NULL comparison for sorting.
// Returns (result, true) if a NULL was involved, (0, false) otherwise.
// When nullsFirst is true, NULLs sort before non-NULLs (result -1 for aIsNil, +1 for bIsNil).
func compareNulls(aIsNil, bIsNil, nullsFirst bool) (int, bool) {
	if !aIsNil && !bIsNil {
		return 0, false
	}
	if aIsNil && bIsNil {
		return 0, true
	}
	if aIsNil {
		if nullsFirst {
			return -1, true
		}
		return 1, true
	}
	// bIsNil
	if nullsFirst {
		return 1, true
	}
	return -1, true
}

// exceptRowsBy returns rows in left that do not appear in right
// (deduplicated), preserving the order from left.
func exceptRowsBy[T any](left, right [][]T, keyFunc func([]T) string) [][]T {
	rightSet := make(map[string]struct{}, len(right))
	for _, row := range right {
		rightSet[keyFunc(row)] = struct{}{}
	}

	seen := make(map[string]struct{}, len(left))
	result := make([][]T, 0, len(left))
	for _, row := range left {
		k := keyFunc(row)
		if _, inRight := rightSet[k]; inRight {
			continue
		}
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		result = append(result, row)
	}
	return result
}
