# Virtual Tables in Anthony SQLite

This document describes virtual table support in Anthony, a pure Go implementation of SQLite. Virtual tables allow you to expose custom data sources through the SQL interface, making non-SQLite data accessible via standard SQL queries.

## Table of Contents

1. [Overview](#overview)
2. [Virtual Table Basics](#virtual-table-basics)
3. [Creating Virtual Tables](#creating-virtual-tables)
4. [Implementing Custom Virtual Tables in Go](#implementing-custom-virtual-tables-in-go)
5. [Built-in Virtual Table Modules](#built-in-virtual-table-modules)
6. [FTS5 Full-Text Search](#fts5-full-text-search)
7. [R-Tree Spatial Indexing](#r-tree-spatial-indexing)
8. [Feature Status](#feature-status)

## Overview

A virtual table is an object that presents a table-like interface to SQL but stores or computes its data in ways other than the standard SQLite B-tree structure. Virtual tables can:

- **Read external data**: CSV files, JSON, remote APIs, or other databases
- **Compute results dynamically**: Generate data on-the-fly based on query parameters
- **Provide specialized indexing**: Full-text search (FTS5) or spatial queries (R-Tree)
- **Expose in-memory structures**: Go data structures accessible via SQL

From an SQL perspective, virtual tables work like regular tables with some limitations:

- No triggers can be created on virtual tables
- Cannot add indices with CREATE INDEX (indices must be built into the implementation)
- Cannot use ALTER TABLE ... ADD COLUMN
- Individual implementations may be read-only or have restricted UPDATE/DELETE support

**Status**: Partially Implemented

## Virtual Table Basics

### Creating a Virtual Table

Virtual tables are created using the `CREATE VIRTUAL TABLE` statement:

```sql
CREATE VIRTUAL TABLE tablename USING modulename;

-- With arguments
CREATE VIRTUAL TABLE tablename USING modulename(arg1, arg2, ...);

-- With schema qualifier
CREATE VIRTUAL TABLE main.tablename USING modulename(arg1, arg2);

-- Temporary virtual tables
CREATE VIRTUAL TABLE temp.tablename USING modulename(arg1, arg2);
```

### Using Virtual Tables

Once created, virtual tables can be queried like regular tables:

```sql
-- Basic query
SELECT * FROM vtable WHERE column = 'value';

-- Joins work
SELECT v.*, t.name
FROM vtable v
JOIN regular_table t ON v.id = t.vtable_id;

-- Aggregation
SELECT COUNT(*), category FROM vtable GROUP BY category;
```

### Dropping Virtual Tables

```sql
DROP TABLE vtable;
```

This calls the module's `Destroy()` method to clean up any persistent state.

### Eponymous Virtual Tables

Some virtual table modules can be used without CREATE VIRTUAL TABLE. These are called "eponymous" virtual tables and exist automatically when the module is registered:

```sql
-- No CREATE VIRTUAL TABLE needed
SELECT * FROM dbstat;  -- If dbstat module is eponymous
```

**Status**: CREATE VIRTUAL TABLE - Implemented; Eponymous tables - Planned

## Creating Virtual Tables

### Example: Temperature Sensor Virtual Table

This example shows a virtual table that exposes temperature sensor readings:

```sql
-- Create a virtual table for temperature sensors
CREATE VIRTUAL TABLE sensors USING temperature_monitor(
    update_interval=5000,
    unit='celsius'
);

-- Query current readings
SELECT sensor_id, temperature, timestamp
FROM sensors
WHERE temperature > 25.0
ORDER BY temperature DESC;
```

### Virtual Table with Column Specification

Some virtual table modules accept column definitions:

```sql
CREATE VIRTUAL TABLE emails USING fts5(
    sender,      -- Column for sender email
    subject,     -- Column for email subject
    body         -- Column for email body
);
```

**Status**: Basic CREATE VIRTUAL TABLE - Implemented

## Implementing Custom Virtual Tables in Go

Anthony provides a Go interface for implementing custom virtual table modules. The implementation is in `internal/vtab/`.

### Module Interface

Every virtual table module implements the `Module` interface:

```go
package vtab

// Module represents a virtual table module that can create/connect to virtual tables.
type Module interface {
    // Create creates a new virtual table instance.
    // Called when CREATE VIRTUAL TABLE is executed.
    Create(db interface{}, moduleName string, dbName string,
           tableName string, args []string) (VirtualTable, string, error)

    // Connect connects to an existing virtual table.
    // Called when a table is opened for use.
    Connect(db interface{}, moduleName string, dbName string,
            tableName string, args []string) (VirtualTable, string, error)
}
```

### VirtualTable Interface

The `VirtualTable` interface defines the virtual table instance:

```go
// VirtualTable represents an instance of a virtual table.
type VirtualTable interface {
    // BestIndex analyzes query constraints and determines the best query plan.
    BestIndex(info *IndexInfo) error

    // Open creates a new cursor for iterating over the virtual table.
    Open() (VirtualCursor, error)

    // Disconnect is called when the last reference to the table is closed.
    Disconnect() error

    // Destroy is called when DROP TABLE is executed.
    Destroy() error

    // Update handles INSERT, UPDATE, and DELETE operations.
    Update(argc int, argv []interface{}) (int64, error)

    // Transaction support (optional)
    Begin() error
    Sync() error
    Commit() error
    Rollback() error

    // Rename is called when the table is renamed (optional).
    Rename(newName string) error
}
```

### VirtualCursor Interface

Cursors iterate over query results:

```go
// VirtualCursor represents a cursor for iterating over virtual table rows.
type VirtualCursor interface {
    // Filter initializes the cursor with query constraints.
    Filter(idxNum int, idxStr string, argv []interface{}) error

    // Next advances to the next row.
    Next() error

    // EOF returns true if the cursor has reached the end.
    EOF() bool

    // Column returns the value of the column at the given index.
    Column(index int) (interface{}, error)

    // Rowid returns the unique rowid for the current row.
    Rowid() (int64, error)

    // Close closes the cursor and releases resources.
    Close() error
}
```

### Example Implementation: Key-Value Store

```go
package main

import (
    "fmt"
    "github.com/JuniperBible/Public.Lib.Anthony/internal/vtab"
    "sync"
)

// KeyValueModule implements a simple key-value store virtual table.
type KeyValueModule struct {
    vtab.BaseModule
    store map[string]string
    mu    sync.RWMutex
}

func NewKeyValueModule() *KeyValueModule {
    return &KeyValueModule{
        store: make(map[string]string),
    }
}

// Create implements vtab.Module.
func (m *KeyValueModule) Create(db interface{}, moduleName, dbName, tableName string,
                                args []string) (vtab.VirtualTable, string, error) {
    // Return the table instance and its schema
    schema := "CREATE TABLE x(key TEXT PRIMARY KEY, value TEXT)"
    return &KeyValueTable{module: m}, schema, nil
}

// Connect implements vtab.Module.
func (m *KeyValueModule) Connect(db interface{}, moduleName, dbName, tableName string,
                                 args []string) (vtab.VirtualTable, string, error) {
    return m.Create(db, moduleName, dbName, tableName, args)
}

// KeyValueTable is the virtual table instance.
type KeyValueTable struct {
    vtab.BaseVirtualTable
    module *KeyValueModule
}

// BestIndex analyzes the query to determine the best access strategy.
func (t *KeyValueTable) BestIndex(info *vtab.IndexInfo) error {
    // Check if we have a key = ? constraint
    for i, c := range info.Constraints {
        if c.Column == 0 && c.Op == vtab.ConstraintEQ && c.Usable {
            // We can use this constraint efficiently
            info.SetConstraintUsage(i, 1, true)
            info.EstimatedCost = 1.0
            info.EstimatedRows = 1
            info.IdxNum = 1 // Signal that we're doing a key lookup
            return nil
        }
    }

    // Full table scan
    info.EstimatedCost = 1000000.0
    info.EstimatedRows = int64(len(t.module.store))
    info.IdxNum = 0
    return nil
}

// Open creates a new cursor.
func (t *KeyValueTable) Open() (vtab.VirtualCursor, error) {
    return &KeyValueCursor{table: t}, nil
}

// Update handles INSERT, UPDATE, DELETE.
func (t *KeyValueTable) Update(argc int, argv []interface{}) (int64, error) {
    t.module.mu.Lock()
    defer t.module.mu.Unlock()

    if argc == 1 {
        // DELETE
        key := argv[0].(string)
        delete(t.module.store, key)
        return 0, nil
    }

    // INSERT or UPDATE
    key := argv[2].(string)
    value := argv[3].(string)
    t.module.store[key] = value
    return 0, nil
}

// KeyValueCursor iterates over key-value pairs.
type KeyValueCursor struct {
    vtab.BaseCursor
    table   *KeyValueTable
    keys    []string
    idx     int
    idxNum  int
    key     string // For key lookups
}

// Filter initializes the cursor with constraints.
func (c *KeyValueCursor) Filter(idxNum int, idxStr string, argv []interface{}) error {
    c.table.module.mu.RLock()
    defer c.table.module.mu.RUnlock()

    c.idxNum = idxNum
    c.idx = 0

    if idxNum == 1 && len(argv) > 0 {
        // Key lookup
        c.key = argv[0].(string)
        if _, exists := c.table.module.store[c.key]; exists {
            c.keys = []string{c.key}
        } else {
            c.keys = nil
        }
    } else {
        // Full scan
        c.keys = make([]string, 0, len(c.table.module.store))
        for k := range c.table.module.store {
            c.keys = append(c.keys, k)
        }
    }

    return nil
}

// Next advances to the next row.
func (c *KeyValueCursor) Next() error {
    c.idx++
    return nil
}

// EOF returns true if we've exhausted the results.
func (c *KeyValueCursor) EOF() bool {
    return c.idx >= len(c.keys)
}

// Column returns the value for a column.
func (c *KeyValueCursor) Column(index int) (interface{}, error) {
    c.table.module.mu.RLock()
    defer c.table.module.mu.RUnlock()

    if c.idx >= len(c.keys) {
        return nil, fmt.Errorf("cursor exhausted")
    }

    key := c.keys[c.idx]
    switch index {
    case 0:
        return key, nil
    case 1:
        return c.table.module.store[key], nil
    default:
        return nil, fmt.Errorf("invalid column index: %d", index)
    }
}

// Rowid returns the row identifier.
func (c *KeyValueCursor) Rowid() (int64, error) {
    return int64(c.idx), nil
}

// Register the module
func init() {
    module := NewKeyValueModule()
    vtab.RegisterModule("kvstore", module)
}
```

### Using the Custom Virtual Table

```go
package main

import (
    "database/sql"
    "log"

    _ "github.com/JuniperBible/Public.Lib.Anthony/internal/driver"
)

func main() {
    db, err := sql.Open("sqlite_internal", ":memory:")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Create the virtual table
    _, err = db.Exec("CREATE VIRTUAL TABLE kv USING kvstore")
    if err != nil {
        log.Fatal(err)
    }

    // Insert data
    _, err = db.Exec("INSERT INTO kv (key, value) VALUES (?, ?)", "name", "Anthony")
    if err != nil {
        log.Fatal(err)
    }

    _, err = db.Exec("INSERT INTO kv (key, value) VALUES (?, ?)", "version", "1.0")
    if err != nil {
        log.Fatal(err)
    }

    // Query data
    var value string
    err = db.QueryRow("SELECT value FROM kv WHERE key = ?", "name").Scan(&value)
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Name: %s\n", value)
}
```

### Query Planning with BestIndex

The `BestIndex()` method is crucial for performance. It analyzes the WHERE clause and ORDER BY to determine the optimal query strategy:

```go
// IndexConstraint represents a WHERE clause constraint.
type IndexConstraint struct {
    Column    int           // Column index (-1 for rowid)
    Op        ConstraintOp  // Operator (=, <, >, <=, >=, MATCH, etc.)
    Usable    bool          // Whether the constraint can be used
    Collation string        // Collation for string comparisons
}

// IndexInfo is passed to BestIndex to describe the query.
type IndexInfo struct {
    // Inputs:
    Constraints []IndexConstraint  // WHERE clause constraints
    OrderBy     []OrderBy          // ORDER BY terms
    ColUsed     uint64             // Bitmask of used columns

    // Outputs (set by BestIndex):
    ConstraintUsage []IndexConstraintUsage  // How to use each constraint
    IdxNum          int                     // Strategy identifier
    IdxStr          string                  // Strategy details
    OrderByConsumed bool                    // Whether we handle ORDER BY
    EstimatedCost   float64                 // Cost estimate (lower is better)
    EstimatedRows   int64                   // Row count estimate
}
```

**Status**: Implemented

## Built-in Virtual Table Modules

### dbstat - Database Statistics (Planned)

The `dbstat` module provides information about the amount of disk space used by database tables and indices:

```sql
-- Eponymous virtual table
SELECT name, path, pageno, pagetype, ncell, payload, unused
FROM dbstat
WHERE name = 'users';
```

**Status**: Planned

### sqlite_schema - Schema Information (Implemented)

Access to database schema metadata:

```sql
SELECT * FROM sqlite_schema WHERE type = 'table';
```

**Status**: Implemented via standard SQLite mechanism

## FTS5 Full-Text Search

FTS5 (Full-Text Search version 5) is a virtual table module that provides advanced full-text search capabilities.

### Overview

FTS5 allows you to efficiently search large collections of text documents. It provides:

- Fast phrase queries
- Prefix matching
- NEAR queries (proximity search)
- Boolean operators (AND, OR, NOT)
- Ranking functions
- Snippet generation and highlighting
- Custom tokenizers

### Creating an FTS5 Table

```sql
CREATE VIRTUAL TABLE documents USING fts5(
    title,       -- Column for document title
    author,      -- Column for author name
    body,        -- Column for document body
    category     -- Column for category
);
```

### Populating FTS5 Tables

FTS5 tables support standard INSERT, UPDATE, and DELETE:

```sql
-- Insert documents
INSERT INTO documents (title, author, body, category)
VALUES ('SQLite Internals', 'Dr. Hipp', 'Deep dive into SQLite...', 'Technical');

INSERT INTO documents (title, author, body, category)
VALUES ('Go Programming', 'Rob Pike', 'Effective Go patterns...', 'Programming');

-- Update documents
UPDATE documents SET category = 'Database' WHERE title = 'SQLite Internals';

-- Delete documents
DELETE FROM documents WHERE rowid = 5;
```

### Full-Text Queries

FTS5 provides three query syntaxes:

```sql
-- MATCH operator
SELECT * FROM documents WHERE documents MATCH 'sqlite';

-- Equals operator
SELECT * FROM documents WHERE documents = 'sqlite';

-- Table-valued function syntax
SELECT * FROM documents('sqlite');
```

### Query Syntax Features

#### Basic Term Search

```sql
-- Find documents containing "sqlite"
SELECT * FROM documents WHERE documents MATCH 'sqlite';

-- Case-insensitive by default
SELECT * FROM documents WHERE documents MATCH 'SQLite';  -- Same as above
```

#### Phrase Queries

```sql
-- Exact phrase match
SELECT * FROM documents WHERE documents MATCH '"virtual table"';

-- Phrase with multiple words
SELECT * FROM documents WHERE documents MATCH '"full text search"';
```

#### Prefix Queries

```sql
-- Words starting with "data"
SELECT * FROM documents WHERE documents MATCH 'data*';

-- Phrase with prefix
SELECT * FROM documents WHERE documents MATCH '"sqlite virtual" + tab*';
```

#### Boolean Operators

```sql
-- AND (both terms required)
SELECT * FROM documents WHERE documents MATCH 'sqlite AND virtual';

-- OR (either term)
SELECT * FROM documents WHERE documents MATCH 'sqlite OR postgres';

-- NOT (exclude documents)
SELECT * FROM documents WHERE documents MATCH 'database NOT nosql';

-- Complex boolean
SELECT * FROM documents WHERE documents MATCH '(sqlite OR postgres) AND (index OR btree)';
```

#### Column Filters

```sql
-- Search specific column
SELECT * FROM documents WHERE documents MATCH 'title:sqlite';

-- Search multiple columns
SELECT * FROM documents WHERE documents MATCH '{title body}:optimization';

-- Exclude column
SELECT * FROM documents WHERE documents MATCH '-category:deprecated';
```

#### NEAR Queries

```sql
-- Words within 10 words of each other
SELECT * FROM documents WHERE documents MATCH 'NEAR(sqlite virtual, 10)';

-- Default proximity (5 words)
SELECT * FROM documents WHERE documents MATCH 'NEAR(database index)';
```

#### Initial Token Queries

```sql
-- Match at start of column
SELECT * FROM documents WHERE documents MATCH '^Introduction';

-- With column filter
SELECT * FROM documents WHERE documents MATCH 'title:^Chapter';
```

### Ranking and Sorting

```sql
-- Sort by relevance (default rank)
SELECT * FROM documents WHERE documents MATCH 'sqlite'
ORDER BY rank;

-- Use BM25 ranking
SELECT *, bm25(documents) AS score
FROM documents WHERE documents MATCH 'virtual table'
ORDER BY score;

-- Custom ranking weights
SELECT *, bm25(documents, 10.0, 5.0, 1.0) AS score
FROM documents WHERE documents MATCH 'sqlite'
ORDER BY score;
```

### Auxiliary Functions

#### highlight() - Highlight Matches

```sql
-- Highlight matches in the body column (column index 2)
SELECT highlight(documents, 2, '<b>', '</b>') AS highlighted_body
FROM documents WHERE documents MATCH 'sqlite';

-- Result: "This is a <b>SQLite</b> database..."
```

#### snippet() - Extract Snippets

```sql
-- Extract relevant snippets from body (column 2)
SELECT snippet(documents, 2, '<b>', '</b>', '...', 32) AS snippet
FROM documents WHERE documents MATCH 'virtual table';

-- Returns: "...implementation of <b>virtual</b> <b>table</b> modules..."
```

#### bm25() - BM25 Ranking

```sql
-- Default BM25 ranking
SELECT title, bm25(documents) AS relevance
FROM documents WHERE documents MATCH 'database'
ORDER BY relevance;

-- Custom column weights (title=10.0, author=5.0, body=1.0, category=2.0)
SELECT title, bm25(documents, 10.0, 5.0, 1.0, 2.0) AS relevance
FROM documents WHERE documents MATCH 'sqlite'
ORDER BY relevance
LIMIT 10;
```

### FTS5 Configuration Options

#### UNINDEXED Columns

```sql
-- Don't index the 'id' column
CREATE VIRTUAL TABLE docs USING fts5(
    id UNINDEXED,
    title,
    body
);
```

#### Prefix Indexes

```sql
-- Create prefix indexes for 2 and 3 character prefixes
CREATE VIRTUAL TABLE docs USING fts5(
    title,
    body,
    prefix='2 3'
);

-- Now prefix queries are faster
SELECT * FROM docs WHERE docs MATCH 'sql*';
```

#### Custom Tokenizers

```sql
-- Use ASCII tokenizer
CREATE VIRTUAL TABLE docs USING fts5(
    content,
    tokenize='ascii'
);

-- Use Porter stemmer
CREATE VIRTUAL TABLE docs USING fts5(
    content,
    tokenize='porter'
);

-- Trigram tokenizer for substring search
CREATE VIRTUAL TABLE docs USING fts5(
    content,
    tokenize='trigram'
);
```

#### Contentless Tables

```sql
-- FTS index without storing the content
CREATE VIRTUAL TABLE docs_idx USING fts5(
    title,
    body,
    content=''
);

-- Store content separately
CREATE TABLE docs_content(
    id INTEGER PRIMARY KEY,
    title TEXT,
    body TEXT
);
```

#### External Content Tables

```sql
-- Create content table
CREATE TABLE articles(
    id INTEGER PRIMARY KEY,
    title TEXT,
    body TEXT
);

-- Create FTS5 index on external content
CREATE VIRTUAL TABLE articles_fts USING fts5(
    title,
    body,
    content='articles',
    content_rowid='id'
);

-- Queries use the FTS index
SELECT * FROM articles_fts WHERE articles_fts MATCH 'search term';
```

### Maintenance Commands

```sql
-- Optimize the FTS5 index (merge segments)
INSERT INTO documents(documents) VALUES('optimize');

-- Rebuild the entire index
INSERT INTO documents(documents) VALUES('rebuild');

-- Check integrity
INSERT INTO documents(documents) VALUES('integrity-check');

-- Delete all data
INSERT INTO documents(documents) VALUES('delete-all');
```

### FTS5 Tokenizers

#### unicode61 (Default)

```sql
CREATE VIRTUAL TABLE docs USING fts5(
    content,
    tokenize='unicode61'
);

-- Options
CREATE VIRTUAL TABLE docs USING fts5(
    content,
    tokenize='unicode61 remove_diacritics 1'
);
```

#### ascii

```sql
CREATE VIRTUAL TABLE docs USING fts5(
    content,
    tokenize='ascii'
);

-- Options
CREATE VIRTUAL TABLE docs USING fts5(
    content,
    tokenize='ascii separators "_-"'
);
```

#### porter

```sql
-- Porter stemming algorithm
CREATE VIRTUAL TABLE docs USING fts5(
    content,
    tokenize='porter unicode61'
);
```

#### trigram

```sql
-- For substring matching
CREATE VIRTUAL TABLE docs USING fts5(
    content,
    tokenize='trigram'
);

-- Find substrings
SELECT * FROM docs WHERE docs MATCH 'abc';  -- Matches "xabcy"
```

### Performance Tips

1. **Use prefix indexes** for prefix queries:
```sql
CREATE VIRTUAL TABLE docs USING fts5(content, prefix='2 3 4');
```

2. **Use contentless tables** if you don't need FTS5 to store the content:
```sql
CREATE VIRTUAL TABLE idx USING fts5(content, content='');
```

3. **Optimize regularly** for large datasets:
```sql
INSERT INTO docs(docs) VALUES('optimize');
```

4. **Use column filters** to narrow searches:
```sql
SELECT * FROM docs WHERE docs MATCH 'title:important';
```

5. **Index only searchable content** using UNINDEXED:
```sql
CREATE VIRTUAL TABLE docs USING fts5(id UNINDEXED, title, body);
```

**Status**: Planned (Not Yet Implemented)

## R-Tree Spatial Indexing

R-Tree is a virtual table module for efficient spatial indexing and range queries.

### Overview

R-Trees are specialized data structures for:

- Geospatial systems (latitude/longitude coordinates)
- CAD systems (3D object boundaries)
- Time-domain queries (event start/end times)
- Any multi-dimensional range data

### Creating an R-Tree Index

```sql
-- 2D spatial index (latitude/longitude)
CREATE VIRTUAL TABLE places USING rtree(
   id,              -- Integer primary key
   min_lat, max_lat,  -- Latitude range
   min_lon, max_lon   -- Longitude range
);

-- 3D spatial index
CREATE VIRTUAL TABLE objects USING rtree(
   id,
   min_x, max_x,
   min_y, max_y,
   min_z, max_z
);

-- Time range index
CREATE VIRTUAL TABLE events USING rtree(
   id,
   start_time, end_time
);
```

### Coordinate Dimensions

```sql
-- 1-dimensional (3 columns: id, min, max)
CREATE VIRTUAL TABLE ranges USING rtree(id, min_val, max_val);

-- 2-dimensional (5 columns)
CREATE VIRTUAL TABLE areas USING rtree(id, min_x, max_x, min_y, max_y);

-- 3-dimensional (7 columns)
CREATE VIRTUAL TABLE volumes USING rtree(id, min_x, max_x, min_y, max_y, min_z, max_z);

-- 4-dimensional (9 columns)
CREATE VIRTUAL TABLE spacetime USING rtree(id, x0, x1, y0, y1, z0, z1, t0, t1);

-- 5-dimensional (11 columns - maximum)
CREATE VIRTUAL TABLE hyper USING rtree(id, a0, a1, b0, b1, c0, c1, d0, d1, e0, e1);
```

### Integer R-Trees

Use `rtree_i32` for integer coordinates:

```sql
CREATE VIRTUAL TABLE grid USING rtree_i32(
   id,
   min_x, max_x,
   min_y, max_y
);
```

### Populating R-Tree Indexes

```sql
-- Insert spatial data (ZIP code boundaries)
INSERT INTO places VALUES
  (28269, 35.272560, 35.407925, -80.851471, -80.735718),
  (28270, 35.059872, 35.161823, -80.794983, -80.728966),
  (28277, 35.001709, 35.101063, -80.876793, -80.767586);

-- Update entries
UPDATE places SET max_lat = 35.41 WHERE id = 28269;

-- Delete entries
DELETE FROM places WHERE id = 28277;
```

### Querying R-Trees

#### Point Containment Query

```sql
-- Find which ZIP code contains a point (35.37785, -80.77470)
SELECT id FROM places
WHERE min_lat <= 35.37785 AND max_lat >= 35.37785
  AND min_lon <= -80.77470 AND max_lon >= -80.77470;
```

#### Overlapping Queries

```sql
-- Find all ZIP codes that overlap with ZIP code 28269
SELECT A.id FROM places AS A, places AS B
WHERE A.max_lat >= B.min_lat AND A.min_lat <= B.max_lat
  AND A.max_lon >= B.min_lon AND A.min_lon <= B.max_lon
  AND B.id = 28269;
```

#### Range Queries

```sql
-- Find all objects within a bounding box
SELECT id FROM places
WHERE max_lat >= 35.0 AND min_lat <= 36.0
  AND max_lon >= -81.0 AND min_lon <= -80.0;
```

#### Partial Constraint Queries

```sql
-- Find all events spanning the 35th parallel
SELECT id FROM events
WHERE max_lat >= 35.0 AND min_lat <= 35.0;
```

### Auxiliary Columns

R-Tree tables can have auxiliary columns (SQLite 3.24.0+):

```sql
CREATE VIRTUAL TABLE places USING rtree(
   id,
   min_lat, max_lat,
   min_lon, max_lon,
   +name TEXT,        -- Auxiliary column
   +population INT,   -- Auxiliary column
   +area REAL         -- Auxiliary column
);

-- Insert with auxiliary data
INSERT INTO places VALUES
  (28269, 35.272560, 35.407925, -80.851471, -80.735718,
   'Charlotte North', 45000, 23.5);

-- Query using auxiliary columns
SELECT name, population FROM places
WHERE min_lat <= 35.37785 AND max_lat >= 35.37785
  AND min_lon <= -80.77470 AND max_lon >= -80.77470;
```

### Custom R-Tree Queries

For advanced geometric queries beyond rectangles:

```sql
-- Register a custom query function (via C API)
-- Then use MATCH operator:
SELECT id FROM shapes WHERE id MATCH circle(45.3, 22.9, 5.0);
SELECT id FROM shapes WHERE id MATCH polygon([...]);
```

### Shadow Tables

R-Tree creates three shadow tables:

```sql
-- For rtree "demo_index":
demo_index_node     -- Tree structure
demo_index_rowid    -- Rowid mapping
demo_index_parent   -- Parent pointers
```

Don't modify these directly.

### Integrity Check

```sql
-- Verify R-Tree integrity
SELECT rtreecheck('places');
-- Returns: 'ok' or error description
```

### Performance Considerations

1. **Roundoff error**: 32-bit floats may slightly expand bounding boxes. For contained-within queries, expand query box by ~0.000012%:

```sql
-- Slightly expand query coordinates
SELECT id FROM places
WHERE min_lat <= 35.37785 * 1.00000012 AND max_lat >= 35.37785 * 0.99999988
  AND min_lon <= -80.77470 * 1.00000012 AND max_lon >= -80.77470 * 0.99999988;
```

2. **Concurrent access**: R-Tree doesn't support simultaneous read/write. If you need to update based on queries, store results first:

```sql
-- DON'T do this (may fail with SQLITE_LOCKED):
UPDATE places SET max_lat = max_lat + 0.5
WHERE max_lat >= 35.0 AND min_lat <= 35.0;

-- DO this instead:
CREATE TEMP TABLE updates AS
SELECT id FROM places WHERE max_lat >= 35.0 AND min_lat <= 35.0;

UPDATE places SET max_lat = max_lat + 0.5
WHERE id IN (SELECT id FROM updates);
```

### Use Cases

#### Geospatial Applications

```sql
-- Store restaurant locations
CREATE VIRTUAL TABLE restaurants USING rtree(
   id, min_lat, max_lat, min_lon, max_lon,
   +name TEXT, +cuisine TEXT, +rating REAL
);

-- Find restaurants near a location
SELECT name, cuisine, rating FROM restaurants
WHERE min_lat <= 35.2277 + 0.01 AND max_lat >= 35.2277 - 0.01
  AND min_lon <= -80.8431 + 0.01 AND max_lon >= -80.8431 - 0.01
ORDER BY rating DESC;
```

#### Time Range Queries

```sql
-- Event scheduling
CREATE VIRTUAL TABLE events USING rtree(
   id, start_time, end_time,
   +title TEXT, +location TEXT
);

-- Find events active during a time range
SELECT title, location FROM events
WHERE start_time <= 1672531200  -- End of query range
  AND end_time >= 1672444800;   -- Start of query range
```

#### CAD Systems

```sql
-- 3D objects in a CAD system
CREATE VIRTUAL TABLE cad_objects USING rtree(
   id, min_x, max_x, min_y, max_y, min_z, max_z,
   +object_type TEXT, +material TEXT
);

-- Find objects in a 3D region
SELECT id, object_type FROM cad_objects
WHERE min_x <= 100 AND max_x >= 50
  AND min_y <= 100 AND max_y >= 50
  AND min_z <= 100 AND max_z >= 50;
```

**Status**: Planned (Not Yet Implemented)

## Feature Status

| Feature | Status | Notes |
|---------|--------|-------|
| **Core Virtual Table Infrastructure** |
| CREATE VIRTUAL TABLE | Implemented | Basic syntax working |
| DROP VIRTUAL TABLE | Implemented | Cleanup via Destroy() |
| Module registration | Implemented | Via vtab.RegisterModule() |
| VirtualTable interface | Implemented | Full Go interface |
| VirtualCursor interface | Implemented | Query iteration support |
| BestIndex query planning | Implemented | Constraint optimization |
| Eponymous virtual tables | Planned | Auto-available modules |
| Eponymous-only virtual tables | Planned | No CREATE required |
| Table-valued functions | Planned | SELECT FROM func(args) |
| **Built-in Modules** |
| dbstat | Planned | Database statistics |
| csv | Planned | CSV file reading |
| json_tree | Planned | JSON data querying |
| **FTS5 Full-Text Search** |
| CREATE VIRTUAL TABLE...fts5 | Planned | Table creation |
| Basic MATCH queries | Planned | Term search |
| Phrase queries | Planned | "exact phrase" |
| Prefix queries | Planned | term* |
| Boolean operators | Planned | AND, OR, NOT |
| Column filters | Planned | column:term |
| NEAR queries | Planned | Proximity search |
| Initial token queries | Planned | ^term |
| BM25 ranking | Planned | Relevance scoring |
| highlight() function | Planned | Match highlighting |
| snippet() function | Planned | Extract snippets |
| Unicode tokenizer | Planned | Default tokenizer |
| ASCII tokenizer | Planned | ASCII-only |
| Porter stemmer | Planned | Word stemming |
| Trigram tokenizer | Planned | Substring search |
| Custom tokenizers | Planned | User-defined |
| Prefix indexes | Planned | Fast prefix queries |
| UNINDEXED columns | Planned | Skip indexing |
| Contentless tables | Planned | Index-only |
| External content tables | Planned | Reference external data |
| Auxiliary functions API | Planned | Custom functions |
| **R-Tree Spatial Indexing** |
| CREATE VIRTUAL TABLE...rtree | Planned | Table creation |
| 1D-5D support | Planned | Up to 5 dimensions |
| rtree_i32 (integer) | Planned | Integer coordinates |
| Point containment | Planned | Point in box queries |
| Overlapping queries | Planned | Box overlap |
| Range queries | Planned | Partial constraints |
| Auxiliary columns | Planned | Extra metadata |
| Custom query functions | Planned | Geometric matching |
| rtreecheck() | Planned | Integrity verification |

## References

- **SQLite Documentation**:
  - [Virtual Tables](https://sqlite.org/vtab.html)
  - [FTS5](https://sqlite.org/fts5.html)
  - [R-Tree](https://sqlite.org/rtree.html)

- **Anthony Implementation**:
  - `internal/vtab/module.go` - Module and VirtualTable interfaces
  - `internal/vtab/index.go` - IndexInfo and constraint types
  - `internal/vtab/registry.go` - Module registration

- **SQLite Source Reference**:
  - `contrib/sqlite/sqlite-doc-3510200/vtab.html` - Virtual table documentation
  - `contrib/sqlite/sqlite-doc-3510200/fts5.html` - FTS5 documentation
  - `contrib/sqlite/sqlite-doc-3510200/rtree.html` - R-Tree documentation

## Next Steps

1. **Implement FTS5 module** - Highest priority for real-world applications
2. **Implement R-Tree module** - For geospatial and range query support
3. **Add eponymous virtual table support** - Convenience feature
4. **Implement built-in utility modules** - dbstat, csv, json_tree
5. **Add table-valued function syntax** - Enhanced usability
6. **Document custom module development** - Developer guide with more examples

---

**Last Updated**: 2026-02-28
**Anthony Version**: In Development
**SQLite Compatibility**: Based on SQLite 3.51.2
