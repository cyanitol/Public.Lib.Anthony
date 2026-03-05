// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
package schema

import (
	"fmt"
	"strings"
	"sync"

	"github.com/JuniperBible/Public.Lib.Anthony/internal/btree"
	"github.com/JuniperBible/Public.Lib.Anthony/internal/pager"
)

// Database represents an attached database with its own schema.
type Database struct {
	Name   string               // Schema name (e.g., "main", "temp", user-defined)
	Path   string               // File path to the database file
	Schema *Schema              // The schema for this database
	Pager  pager.PagerInterface // Supports both file and memory pagers
	Btree  *btree.Btree
}

// DatabaseRegistry manages multiple attached databases per connection.
type DatabaseRegistry struct {
	databases map[string]*Database // map from schema name to database
	mu        sync.RWMutex
}

// NewDatabaseRegistry creates a new database registry.
func NewDatabaseRegistry() *DatabaseRegistry {
	return &DatabaseRegistry{
		databases: make(map[string]*Database),
	}
}

// AttachDatabase attaches a database with the given schema name and file path.
func (dr *DatabaseRegistry) AttachDatabase(schemaName, filePath string, p pager.PagerInterface, bt *btree.Btree) error {
	dr.mu.Lock()
	defer dr.mu.Unlock()

	// Check if schema name already exists
	lowerName := strings.ToLower(schemaName)
	if _, exists := dr.databases[lowerName]; exists {
		return fmt.Errorf("database %s is already attached", schemaName)
	}

	// Create new database entry
	db := &Database{
		Name:   schemaName,
		Path:   filePath,
		Schema: NewSchema(),
		Pager:  p,
		Btree:  bt,
	}

	// Load schema from the database file
	if bt != nil {
		if err := db.Schema.LoadFromMaster(bt); err != nil {
			// Ignore errors for empty databases
		}
	}

	dr.databases[lowerName] = db
	return nil
}

// DetachDatabase detaches a database by schema name.
func (dr *DatabaseRegistry) DetachDatabase(schemaName string) error {
	dr.mu.Lock()
	defer dr.mu.Unlock()

	lowerName := strings.ToLower(schemaName)

	// Cannot detach main or temp databases
	if lowerName == "main" || lowerName == "temp" {
		return fmt.Errorf("cannot detach database %s", schemaName)
	}

	db, exists := dr.databases[lowerName]
	if !exists {
		return fmt.Errorf("no such database: %s", schemaName)
	}

	// Close the database pager
	if db.Pager != nil {
		if err := db.Pager.Close(); err != nil {
			return fmt.Errorf("failed to close database: %w", err)
		}
	}

	delete(dr.databases, lowerName)
	return nil
}

// GetDatabase retrieves a database by schema name.
func (dr *DatabaseRegistry) GetDatabase(schemaName string) (*Database, bool) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	lowerName := strings.ToLower(schemaName)
	db, ok := dr.databases[lowerName]
	return db, ok
}

// GetTable retrieves a table by name, optionally qualified with schema.
// If schemaName is empty, searches "main" database first, then others.
func (dr *DatabaseRegistry) GetTable(schemaName, tableName string) (*Table, string, bool) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if schemaName != "" {
		return dr.getQualifiedTable(schemaName, tableName)
	}

	return dr.searchUnqualifiedTable(tableName)
}

// getQualifiedTable retrieves a table using a qualified schema.table name
func (dr *DatabaseRegistry) getQualifiedTable(schemaName, tableName string) (*Table, string, bool) {
	lowerSchema := strings.ToLower(schemaName)
	db, ok := dr.databases[lowerSchema]
	if !ok {
		return nil, "", false
	}
	table, ok := db.Schema.GetTable(tableName)
	return table, schemaName, ok
}

// searchUnqualifiedTable searches for a table across all databases in priority order
func (dr *DatabaseRegistry) searchUnqualifiedTable(tableName string) (*Table, string, bool) {
	searchOrder := dr.buildSearchOrder()

	for _, dbName := range searchOrder {
		if table, ok := dr.findTableInDatabase(dbName, tableName); ok {
			return table, dbName, true
		}
	}

	return nil, "", false
}

// buildSearchOrder creates the database search priority: main, temp, then others
func (dr *DatabaseRegistry) buildSearchOrder() []string {
	searchOrder := []string{"main", "temp"}
	for name := range dr.databases {
		if name != "main" && name != "temp" {
			searchOrder = append(searchOrder, name)
		}
	}
	return searchOrder
}

// findTableInDatabase looks up a table in a specific database
func (dr *DatabaseRegistry) findTableInDatabase(dbName, tableName string) (*Table, bool) {
	db, ok := dr.databases[dbName]
	if !ok {
		return nil, false
	}
	return db.Schema.GetTable(tableName)
}

// ListDatabases returns a list of all attached database names.
func (dr *DatabaseRegistry) ListDatabases() []string {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	names := make([]string, 0, len(dr.databases))
	for name := range dr.databases {
		names = append(names, name)
	}
	return names
}

// GetMainDatabase returns the main database.
func (dr *DatabaseRegistry) GetMainDatabase() (*Database, bool) {
	return dr.GetDatabase("main")
}
