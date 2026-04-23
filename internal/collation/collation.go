// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
// Package collation provides collation sequences for string comparison operations.
// This is a low-level package with no dependencies on other internal packages
// except for utf, which provides the actual comparison implementations.
package collation

import (
	"fmt"
	"strings"
	"sync"

	"github.com/cyanitol/Public.Lib.Anthony/internal/utf"
)

// CollationFunc is a function type for custom collation comparison.
// It compares two strings and returns:
//
//	-1 if a < b
//	 0 if a == b
//	+1 if a > b
type CollationFunc func(a, b string) int

// Collation represents a collation sequence used for string comparisons.
type Collation struct {
	Name string        // Collation name (e.g., "BINARY", "NOCASE", "RTRIM")
	Func CollationFunc // Comparison function
}

// CollationRegistry manages registered collation sequences.
type CollationRegistry struct {
	collations map[string]*Collation
	mu         sync.RWMutex
}

var (
	// globalRegistry is the global collation registry.
	globalRegistry = NewCollationRegistry()
)

// NewCollationRegistry creates a new collation registry with built-in collations.
func NewCollationRegistry() *CollationRegistry {
	cr := &CollationRegistry{
		collations: make(map[string]*Collation),
	}

	// Register built-in collations
	cr.registerBuiltinCollations()

	return cr
}

// registerBuiltinCollations registers SQLite's standard collation sequences.
func (cr *CollationRegistry) registerBuiltinCollations() {
	// BINARY: Byte-by-byte comparison (case-sensitive, default)
	cr.collations["BINARY"] = &Collation{
		Name: "BINARY",
		Func: utf.CompareBinary,
	}

	// NOCASE: Case-insensitive comparison for ASCII characters (A-Z = a-z)
	// Note: Only ASCII characters are folded, matching SQLite behavior
	cr.collations["NOCASE"] = &Collation{
		Name: "NOCASE",
		Func: utf.CompareNoCase,
	}

	// RTRIM: Ignores trailing spaces during comparison
	cr.collations["RTRIM"] = &Collation{
		Name: "RTRIM",
		Func: utf.CompareRTrim,
	}
}

// Register registers a custom collation sequence.
// If a collation with the same name already exists, it will be replaced.
func (cr *CollationRegistry) Register(name string, fn CollationFunc) error {
	if name == "" {
		return fmt.Errorf("collation name cannot be empty")
	}
	if fn == nil {
		return fmt.Errorf("collation function cannot be nil")
	}

	cr.mu.Lock()
	defer cr.mu.Unlock()

	key := normalizeCollationName(name)
	cr.collations[key] = &Collation{
		Name: name,
		Func: fn,
	}

	return nil
}

// Get retrieves a collation by name.
// Returns the collation and true if found, nil and false otherwise.
// Collation names are case-insensitive.
func (cr *CollationRegistry) Get(name string) (*Collation, bool) {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	coll, ok := cr.collations[normalizeCollationName(name)]
	return coll, ok
}

// Unregister removes a collation from the registry.
// Built-in collations (BINARY, NOCASE, RTRIM) cannot be unregistered.
func (cr *CollationRegistry) Unregister(name string) error {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	key := normalizeCollationName(name)

	// Protect built-in collations
	if key == "BINARY" || key == "NOCASE" || key == "RTRIM" {
		return fmt.Errorf("cannot unregister built-in collation: %s", name)
	}

	delete(cr.collations, key)
	return nil
}

func normalizeCollationName(name string) string {
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c >= 'a' && c <= 'z') || c >= 0x80 {
			return strings.ToUpper(name)
		}
	}
	return name
}

// List returns a list of all registered collation names.
func (cr *CollationRegistry) List() []string {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	names := make([]string, 0, len(cr.collations))
	for name := range cr.collations {
		names = append(names, name)
	}

	return names
}

// GlobalRegistry returns the global collation registry.
func GlobalRegistry() *CollationRegistry {
	return globalRegistry
}

// RegisterCollation registers a custom collation in the global registry.
func RegisterCollation(name string, fn CollationFunc) error {
	return globalRegistry.Register(name, fn)
}

// GetCollation retrieves a collation from the global registry.
func GetCollation(name string) (*Collation, bool) {
	return globalRegistry.Get(name)
}

// UnregisterCollation removes a collation from the global registry.
func UnregisterCollation(name string) error {
	return globalRegistry.Unregister(name)
}

// Compare compares two strings using the specified collation.
// If the collation is empty or not found, BINARY collation is used.
func Compare(a, b string, collationName string) int {
	if collationName == "" {
		collationName = "BINARY"
	}

	coll, ok := GetCollation(collationName)
	if !ok {
		// Fall back to BINARY if collation not found
		coll, _ = GetCollation("BINARY")
	}

	return coll.Func(a, b)
}

// CompareBytes compares two byte slices using the specified collation.
// If the collation is empty or not found, BINARY collation is used.
func CompareBytes(a, b []byte, collationName string) int {
	return Compare(string(a), string(b), collationName)
}

// GetCollationFunc retrieves just the comparison function for a collation.
// Returns nil if the collation is not found.
func GetCollationFunc(name string) CollationFunc {
	coll, ok := GetCollation(name)
	if !ok {
		return nil
	}
	return coll.Func
}

// DefaultCollation returns the default collation name (BINARY).
func DefaultCollation() string {
	return "BINARY"
}
