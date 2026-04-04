// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
package schema

import "strings"

// findCaseInsensitive performs a case-insensitive lookup in a string-keyed map.
// Returns the value and true if found, zero value and false otherwise.
func findCaseInsensitive[V any](m map[string]V, name string) (V, bool) {
	lowerName := strings.ToLower(name)
	for key, val := range m {
		if strings.ToLower(key) == lowerName {
			return val, true
		}
	}
	var zero V
	return zero, false
}

// deleteCaseInsensitive performs a case-insensitive delete in a string-keyed map.
// Returns true if an entry was deleted, false otherwise.
func deleteCaseInsensitive[V any](m map[string]V, name string) bool {
	lowerName := strings.ToLower(name)
	for key := range m {
		if strings.ToLower(key) == lowerName {
			delete(m, key)
			return true
		}
	}
	return false
}

// keyCaseInsensitive returns the actual map key matching name case-insensitively.
// Returns the key and true if found, empty string and false otherwise.
func keyCaseInsensitive[V any](m map[string]V, name string) (string, bool) {
	lowerName := strings.ToLower(name)
	for key := range m {
		if strings.ToLower(key) == lowerName {
			return key, true
		}
	}
	return "", false
}

// existsCaseInsensitive checks if a key exists in a map (case-insensitive).
func existsCaseInsensitive[V any](m map[string]V, name string) bool {
	_, found := findCaseInsensitive(m, name)
	return found
}
