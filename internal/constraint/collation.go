// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
// Package constraint provides constraint-related functionality for SQLite databases.
// Collation sequences have been moved to the internal/collation package to avoid
// import cycles. This file now provides re-exports for backwards compatibility.
package constraint

import (
	"github.com/JuniperBible/Public.Lib.Anthony/internal/collation"
)

// Re-export types from the collation package for backwards compatibility
type (
	CollationFunc     = collation.CollationFunc
	Collation         = collation.Collation
	CollationRegistry = collation.CollationRegistry
)

// Re-export functions from the collation package
var (
	NewCollationRegistry = collation.NewCollationRegistry
	GlobalRegistry       = collation.GlobalRegistry
	RegisterCollation    = collation.RegisterCollation
	GetCollation         = collation.GetCollation
	UnregisterCollation  = collation.UnregisterCollation
	Compare              = collation.Compare
	CompareBytes         = collation.CompareBytes
	GetCollationFunc     = collation.GetCollationFunc
	DefaultCollation     = collation.DefaultCollation
)
