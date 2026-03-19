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
