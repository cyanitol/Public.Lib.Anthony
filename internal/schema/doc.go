// SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0)
// Package schema provides comprehensive schema management for the pure Go SQLite database engine.
//
// This package is a pure Go implementation based on SQLite source code.
// SQLite is in the public domain: https://sqlite.org/copyright.html
//
// This package implements SQLite's schema tracking, including tables, indexes, views, and triggers.
// It manages the sqlite_master table and provides type affinity rules per SQLite specification.
//
// # Overview
//
// The schema package consists of three main components:
//
//  1. Schema Manager (schema.go) - Thread-safe schema tracking and manipulation
//  2. SQLite Master Table (master.go) - Reading/writing schema from/to sqlite_master
//  3. Type Affinity (affinity.go) - SQLite type affinity rules and determination
//
// # Schema Manager
//
// The Schema type provides a thread-safe container for database schema information:
//
//	schema := schema.NewSchema()
//
//	// Create a table from a parsed CREATE TABLE statement
//	table, err := schema.CreateTable(createTableStmt)
//
//	// Retrieve a table (case-insensitive)
//	table, ok := schema.GetTable("users")
//
//	// List all tables
//	tables := schema.ListTables()
//
//	// Drop a table and all its indexes
//	err = schema.DropTable("users")
//
// # Type Affinity
//
// SQLite uses type affinity to determine how values should be stored and compared.
// The package implements the five type affinities:
//
//   - TEXT: String data
//   - NUMERIC: Numbers that may contain decimals
//   - INTEGER: Whole numbers
//   - REAL: Floating-point numbers
//   - BLOB: Binary data
//
// Type affinity is determined from the declared column type using SQLite's rules:
//
//	affinity := schema.DetermineAffinity("VARCHAR(100)")  // Returns AffinityText
//	affinity := schema.DetermineAffinity("INTEGER")       // Returns AffinityInteger
//	affinity := schema.DetermineAffinity("DECIMAL(10,2)") // Returns AffinityNumeric
//
// # SQLite Master Table
//
// The sqlite_master table stores all schema information in SQLite databases:
//
//	// Initialize sqlite_master in a new database
//	err := schema.InitializeMaster()
//
//	// Load schema from an existing database
//	err = schema.LoadFromMaster(btree)
//
//	// Save current schema to sqlite_master
//	err = schema.SaveToMaster(btree)
//
// # Table Structure
//
// Tables track columns with their type information, constraints, and metadata:
//
//	type Table struct {
//	    Name         string           // Table name
//	    RootPage     uint32           // B-tree root page number
//	    SQL          string           // CREATE TABLE statement
//	    Columns      []*Column        // Column definitions
//	    PrimaryKey   []string         // Primary key column names
//	    WithoutRowID bool             // WITHOUT ROWID table
//	    Strict       bool             // STRICT table (SQLite 3.37+)
//	}
//
// # Column Structure
//
// Columns store detailed type and constraint information:
//
//	type Column struct {
//	    Name     string      // Column name
//	    Type     string      // Declared type (e.g., "INTEGER", "TEXT")
//	    Affinity Affinity    // Computed type affinity
//	    NotNull  bool        // NOT NULL constraint
//	    Default  interface{} // Default value
//
//	    // Additional constraints
//	    PrimaryKey    bool   // Part of PRIMARY KEY
//	    Unique        bool   // UNIQUE constraint
//	    Autoincrement bool   // AUTOINCREMENT
//	    Generated     bool   // Generated column
//	}
//
// # Index Structure
//
// Indexes track indexed columns and their properties:
//
//	type Index struct {
//	    Name     string   // Index name
//	    Table    string   // Associated table name
//	    RootPage uint32   // B-tree root page number
//	    SQL      string   // CREATE INDEX statement
//	    Columns  []string // Indexed column names
//	    Unique   bool     // UNIQUE index
//	    Partial  bool     // Partial index (has WHERE clause)
//	    Where    string   // WHERE clause for partial indexes
//	}
//
// # Thread Safety
//
// All Schema methods are thread-safe. Concurrent reads and writes are handled
// with a sync.RWMutex, allowing multiple concurrent readers or one writer.
//
// # SQLite Compatibility
//
// This implementation follows SQLite's behavior for:
//
//   - Case-insensitive table and column names
//   - Type affinity rules (https://sqlite.org/datatype3.html)
//   - sqlite_master table structure
//   - WITHOUT ROWID tables
//   - STRICT tables (SQLite 3.37+)
//   - Generated columns (SQLite 3.31+)
//
// # Example Usage
//
//	// Create a new schema
//	s := schema.NewSchema()
//
//	// Initialize sqlite_master table
//	if err := s.InitializeMaster(); err != nil {
//	    log.Fatal(err)
//	}
//
//	// Parse and create a table
//	parser := parser.NewParser("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
//	stmts, err := parser.Parse()
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	table, err := s.CreateTable(stmts[0].(*parser.CreateTableStmt))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Access table information
//	fmt.Printf("Table: %s\n", table.Name)
//	fmt.Printf("Columns: %d\n", len(table.Columns))
//
//	for _, col := range table.Columns {
//	    fmt.Printf("  %s %s (affinity: %s)\n",
//	        col.Name, col.Type, schema.AffinityName(col.Affinity))
//	}
//
//	// Create an index
//	indexParser := parser.NewParser("CREATE INDEX idx_users_name ON users(name)")
//	indexStmts, _ := indexParser.Parse()
//	index, err := s.CreateIndex(indexStmts[0].(*parser.CreateIndexStmt))
//
//	// List all indexes for a table
//	indexes := s.GetTableIndexes("users")
//	fmt.Printf("Indexes on users: %d\n", len(indexes))
//
// # Implementation Notes
//
// The current implementation provides a complete in-memory schema manager with
// placeholder methods for sqlite_master serialization. Full serialization support
// requires integration with the record encoder/decoder from the sql package.
//
// Future enhancements may include:
//   - Complete sqlite_master serialization/deserialization
//   - View and trigger support
//   - Foreign key constraint tracking
//   - Full-text search index support
//   - Virtual table support
package schema
