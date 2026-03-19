// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
// Package anthony provides a pure Go SQLite database driver.
//
// Import this package to register the "sqlite_internal" driver with database/sql:
//
//	import _ "github.com/cyanitol/Public.Lib.Anthony"
//
// Then open a database:
//
//	db, err := sql.Open("sqlite_internal", "mydb.sqlite")
//
// Or use the convenience functions:
//
//	db, err := anthony.Open("mydb.sqlite")
package anthony

import (
	"database/sql"

	"github.com/cyanitol/Public.Lib.Anthony/internal/collation"
	_ "github.com/cyanitol/Public.Lib.Anthony/internal/driver" // Register driver
)

// DriverName is the name registered with database/sql.
const DriverName = "sqlite_internal"

// Open opens a SQLite database using the anthony driver.
func Open(dataSourceName string) (*sql.DB, error) {
	return sql.Open(DriverName, dataSourceName)
}

// OpenReadOnly opens a SQLite database in read-only mode.
func OpenReadOnly(path string) (*sql.DB, error) {
	return Open(path + "?mode=ro")
}

// CollationFunc is a comparison function for custom collation sequences.
// It compares two strings and returns -1, 0, or 1.
type CollationFunc = collation.CollationFunc

// RegisterCollation registers a custom collation in the global registry.
// Collations registered globally are available to all connections.
//
// For per-connection collations, use Conn.CreateCollation() via sql.Conn.Raw().
//
// The comparison function must return:
//
//	-1 if a < b
//	 0 if a == b
//	+1 if a > b
//
// Example:
//
//	anthony.RegisterCollation("REVERSE", func(a, b string) int {
//	    if a > b { return -1 }
//	    if a < b { return 1 }
//	    return 0
//	})
func RegisterCollation(name string, fn CollationFunc) error {
	return collation.RegisterCollation(name, fn)
}

// UnregisterCollation removes a custom collation from the global registry.
// Built-in collations (BINARY, NOCASE, RTRIM) cannot be removed.
func UnregisterCollation(name string) error {
	return collation.UnregisterCollation(name)
}
