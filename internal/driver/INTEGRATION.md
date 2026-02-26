# SQLite Driver Integration Architecture

This document describes how the database/sql driver integrates all internal SQLite components.

## Component Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                     database/sql Package                     │
│                   (Standard Go Library)                      │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    driver Package                            │
│  ┌──────────┬──────────┬──────────┬──────────┬──────────┐  │
│  │  Driver  │   Conn   │   Stmt   │   Rows   │    Tx    │  │
│  └──────────┴──────────┴──────────┴──────────┴──────────┘  │
└────┬────────┬────────┬────────┬────────┬────────┬──────────┘
     │        │        │        │        │        │
     ▼        ▼        ▼        ▼        ▼        ▼
┌────────┐ ┌──────┐ ┌────────┐ ┌──────┐ ┌────┐ ┌─────────┐
│ Parser │ │ VDBE │ │ Pager  │ │Btree │ │Expr│ │Functions│
└────────┘ └──────┘ └────────┘ └──────┘ └────┘ └─────────┘
     │        │          │         │       │        │
     └────────┴──────────┴─────────┴───────┴────────┘
                         │
                    ┌────┴────┐
                    │   UTF   │
                    └─────────┘
```

## Data Flow

### 1. Opening a Connection

```
Application
    │
    ├─ sql.Open("sqlite", "database.db")
    │
    └─► Driver.Open()
         │
         ├─ Parse DSN (data source name)
         │
         ├─ pager.Open(filename)
         │   │
         │   ├─ Opens file handle
         │   ├─ Reads database header
         │   ├─ Initializes page cache
         │   └─ Returns Pager instance
         │
         ├─ btree.NewBtree(pageSize)
         │   └─ Initializes B-tree layer
         │
         └─► Returns Conn
              └─ Contains: pager, btree, metadata
```

### 2. Preparing a Statement

```
Application
    │
    ├─ db.Prepare("SELECT * FROM users WHERE id = ?")
    │
    └─► Conn.Prepare()
         │
         ├─ parser.Parse(sql)
         │   │
         │   ├─ Tokenization (lexer)
         │   ├─ Syntax analysis
         │   └─ Returns AST
         │
         └─► Returns Stmt
              └─ Contains: conn, query, AST
```

### 3. Executing a Query

```
Application
    │
    ├─ stmt.Query(42)
    │
    └─► Stmt.QueryContext()
         │
         ├─ Stmt.compile()
         │   │
         │   ├─ Analyze AST
         │   │
         │   ├─ planner.Plan(ast)
         │   │   ├─ Table lookup
         │   │   ├─ Index selection
         │   │   ├─ Cost estimation
         │   │   └─ Returns QueryPlan
         │   │
         │   ├─ Generate VDBE bytecode
         │   │   ├─ OpInit
         │   │   ├─ OpOpenRead (cursor)
         │   │   ├─ OpRewind
         │   │   ├─ OpColumn (extract columns)
         │   │   ├─ OpResultRow
         │   │   ├─ OpNext (loop)
         │   │   └─ OpHalt
         │   │
         │   └─ Bind parameters
         │
         └─► Returns Rows
              └─ Contains: vdbe, columns
```

### 4. Fetching Results

```
Application
    │
    ├─ rows.Next()
    │
    └─► Rows.Next()
         │
         ├─ VDBE.Step()
         │   │
         │   ├─ Fetch next instruction
         │   │
         │   ├─ Execute instruction
         │   │   │
         │   │   └─ For each opcode:
         │   │       ├─ OpOpenRead → btree.OpenCursor()
         │   │       ├─ OpRewind → cursor.First()
         │   │       ├─ OpColumn → cursor.GetColumn()
         │   │       ├─ OpResultRow → Set result
         │   │       └─ OpNext → cursor.Next()
         │   │
         │   └─ Returns result row
         │
         ├─ Convert vdbe.Mem to driver.Value
         │   ├─ NULL → nil
         │   ├─ INTEGER → int64
         │   ├─ REAL → float64
         │   ├─ TEXT → string
         │   └─ BLOB → []byte
         │
         └─► Populate dest[]
```

### 5. Transaction Lifecycle

```
Application
    │
    ├─ tx := db.Begin()
    │
    └─► Conn.BeginTx()
         │
         ├─ Set conn.inTx = true
         │
         └─► Returns Tx

Application
    │
    ├─ tx.Commit()
    │
    └─► Tx.Commit()
         │
         └─ pager.Commit()
             │
             ├─ Write dirty pages to file
             ├─ Sync database file
             ├─ Finalize journal
             └─ Release locks

OR

Application
    │
    ├─ tx.Rollback()
    │
    └─► Tx.Rollback()
         │
         └─ pager.Rollback()
             │
             ├─ Read journal file
             ├─ Restore original pages
             ├─ Clear page cache
             └─ Delete journal
```

## Module Responsibilities

### Driver (`driver.go`)

- **Purpose**: Entry point for database/sql
- **Responsibilities**:
  - Register with sql.Register()
  - Parse connection strings
  - Manage connection pool
  - Create new connections

### Connection (`conn.go`)

- **Purpose**: Represents a database connection
- **Responsibilities**:
  - Own pager and btree instances
  - Prepare SQL statements
  - Manage transactions
  - Connection lifecycle

### Statement (`stmt.go`)

- **Purpose**: Represents a prepared SQL statement
- **Responsibilities**:
  - Compile SQL to VDBE bytecode
  - Bind parameters
  - Execute queries
  - Manage result sets

### Rows (`rows.go`)

- **Purpose**: Iterator over query results
- **Responsibilities**:
  - Step VDBE to get next row
  - Convert VDBE memory to Go values
  - Handle EOF conditions
  - Resource cleanup

### Transaction (`tx.go`)

- **Purpose**: Manages transaction boundaries
- **Responsibilities**:
  - Coordinate with pager for ACID
  - Commit changes atomically
  - Rollback on errors
  - Lock management

### Value (`value.go`)

- **Purpose**: Type conversion utilities
- **Responsibilities**:
  - Convert Go types to SQLite values
  - Handle NULL values
  - Implement driver.Result
  - Type safety

## VDBE Bytecode Generation

The statement compiler generates VDBE bytecode for each SQL statement type:

### SELECT Statement
```
Init 0 5 0                  # Initialize, jump to 5 if empty
OpenRead 0 2 0              # Open cursor 0 on root page 2
Rewind 0 10 0               # Rewind cursor, jump to 10 if empty
Column 0 1 1                # Read column 1 into register 1
Column 0 2 2                # Read column 2 into register 2
ResultRow 1 2               # Output registers 1-2
Next 0 3                    # Loop back to 3
Close 0                     # Close cursor
Halt 0 0 0                  # Success
```

### INSERT Statement
```
Init 0 5 0                  # Initialize
OpenWrite 0 2 0             # Open write cursor
Integer 42 1                # Value into register 1
String "John" 2             # Value into register 2
MakeRecord 1 2 3            # Create record in register 3
NewRowid 0 4                # Get new rowid
Insert 0 3 4                # Insert record
Close 0                     # Close cursor
Halt 0 0 0                  # Success
```

## Error Handling

Errors propagate through the stack:

1. **Low-level errors** (pager, btree) → Wrapped and returned
2. **VDBE errors** → Set error message, halt execution
3. **Driver errors** → Return as Go error to application
4. **Transaction errors** → Trigger automatic rollback

## Thread Safety

- **Driver**: Thread-safe (uses mutex)
- **Connection**: NOT thread-safe (per sql/database spec)
- **Statement**: NOT thread-safe (per sql/database spec)
- **Pager**: Thread-safe (uses mutex)

Applications should use connection pooling via database/sql.

## Future Enhancements

1. **Query Optimization**
   - Advanced cost-based optimization
   - Index recommendations
   - Query rewriting

2. **Concurrency**
   - WAL (Write-Ahead Logging) mode
   - Reader-writer locks
   - Parallel query execution

3. **Advanced Features**
   - Virtual tables
   - Full-text search
   - JSON functions
   - Window functions

4. **Performance**
   - Compiled expressions
   - JIT compilation of hot bytecode
   - Better caching strategies

## Related Documentation

- [SQLite Documentation](https://sqlite.org/docs.html)
- [database/sql Package](https://pkg.go.dev/database/sql)
- [VDBE Opcodes](../vdbe/README.md)
- [B-tree Structure](../btree/README.md)
- [Pager Layer](../pager/README.md)
