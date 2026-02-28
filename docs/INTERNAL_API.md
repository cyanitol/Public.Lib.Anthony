# Anthony SQLite Internal API Documentation

This document provides comprehensive documentation of Anthony's internal package architecture, designed for contributors and maintainers who need to understand the implementation details.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Package Hierarchy](#package-hierarchy)
3. [Core Packages](#core-packages)
   - [driver](#driver-package)
   - [btree](#btree-package)
   - [pager](#pager-package)
   - [vdbe](#vdbe-package)
   - [parser](#parser-package)
   - [schema](#schema-package)
   - [functions](#functions-package)
   - [security](#security-package)
4. [Supporting Packages](#supporting-packages)
5. [Data Flow](#data-flow)
6. [Transaction Architecture](#transaction-architecture)
7. [Extension Points](#extension-points)

---

## Architecture Overview

Anthony implements a SQLite-compatible database engine in pure Go, following a layered architecture inspired by SQLite's design:

```
┌─────────────────────────────────────────────────────┐
│              database/sql Interface                 │
│                (Go standard library)                │
└───────────────────┬─────────────────────────────────┘
                    │
┌───────────────────▼─────────────────────────────────┐
│                  driver Package                     │
│  (Conn, Stmt, Rows - database/sql/driver impl)     │
└───┬───────────────────────────────────────────┬─────┘
    │                                           │
    │  ┌────────────────────────────────────┐  │
    │  │         parser Package             │  │
    │  │    (SQL parsing and AST)           │  │
    │  └────────────────┬───────────────────┘  │
    │                   │                       │
    │  ┌────────────────▼───────────────────┐  │
    │  │        planner Package             │  │
    │  │   (Query planning & optimization)  │  │
    │  └────────────────┬───────────────────┘  │
    │                   │                       │
    │  ┌────────────────▼───────────────────┐  │
    │  │         engine Package             │  │
    │  │    (Compiles AST to VDBE code)     │  │
    │  └────────────────┬───────────────────┘  │
    │                   │                       │
┌───▼───────────────────▼───────────────────────▼─────┐
│                   vdbe Package                      │
│         (Virtual Database Engine - bytecode)        │
└───┬─────────────────┬─────────────────┬─────────────┘
    │                 │                 │
┌───▼────┐   ┌────────▼────────┐   ┌───▼──────────┐
│ schema │   │    functions    │   │   btree      │
│        │   │   (SQL funcs)   │   │ (B+ tree)    │
└────────┘   └─────────────────┘   └───┬──────────┘
                                       │
                               ┌───────▼──────────┐
                               │     pager        │
                               │ (Page cache,     │
                               │  journaling,     │
                               │  transactions)   │
                               └───────┬──────────┘
                                       │
                               ┌───────▼──────────┐
                               │   File System    │
                               └──────────────────┘
```

---

## Package Hierarchy

### Dependency Layers

**Layer 1: Foundation**
- `security` - Path validation, arithmetic overflow protection, resource limits
- `utf` - UTF-8/UTF-16 handling, varint encoding
- `format` - Binary data formatting

**Layer 2: Storage**
- `pager` - Page-level I/O, caching, journaling, WAL, transactions
- `btree` - B+ tree implementation on top of pager

**Layer 3: Schema & Data**
- `schema` - Table/index definitions, affinity, metadata
- `expr` - Expression evaluation, type affinity, comparisons
- `functions` - Built-in SQL functions (scalar, aggregate, window)

**Layer 4: Execution**
- `vdbe` - Virtual machine executing bytecode
- `parser` - SQL parsing to AST
- `planner` - Query planning and optimization
- `engine` - AST compilation to VDBE bytecode

**Layer 5: Driver Interface**
- `driver` - database/sql/driver implementation (Conn, Stmt, Rows)

---

## Core Packages

### driver Package

**Location**: `internal/driver/`

**Purpose**: Implements Go's `database/sql/driver` interface, providing the public API for database operations.

#### Key Types

##### Driver
```go
type Driver struct {
    mu          sync.Mutex
    conns       map[string]*Conn      // Connection pool
    dbs         map[string]*dbState   // Shared database state per file
    memoryCount int64                 // Counter for unique in-memory DBs
}
```

- **Singleton Pattern**: Registered as `"sqlite_internal"` in `database/sql`
- **Connection Pooling**: Manages multiple connections to same database file
- **Shared State**: `dbState` contains pager, btree, and schema shared across connections

##### Conn
```go
type Conn struct {
    driver         *Driver
    filename       string
    pager          pager.PagerInterface
    btree          *btree.Btree
    schema         *schema.Schema
    funcReg        *functions.Registry
    dbRegistry     *schema.DatabaseRegistry
    stmts          map[*Stmt]struct{}
    inTx           bool
    securityConfig *security.SecurityConfig
}
```

**Responsibilities**:
- Connection lifecycle management
- Transaction control (`Begin`, `Commit`, `Rollback`)
- Statement preparation
- User-defined function registration
- Security sandbox enforcement

**Thread Safety**: Uses `sync.Mutex` for concurrent access protection

##### Stmt
```go
type Stmt struct {
    conn   *Conn
    query  string
    ast    parser.Statement
    vdbe   *vdbe.VDBE
    closed bool
    mu     sync.Mutex
}
```

**Execution Flow**:
1. Parse SQL to AST (via `parser`)
2. Compile AST to VDBE bytecode (via `engine`)
3. Execute VDBE program
4. Return results (via `Rows` for queries)

##### Rows
```go
type Rows struct {
    stmt    *Stmt
    vdbe    *vdbe.VDBE
    columns []string
    ctx     context.Context
    closed  bool
}
```

**Integration with VDBE**:
- Calls `vdbe.Step()` to execute one step of bytecode
- Waits for `StateRowReady` to retrieve result row
- Returns `io.EOF` when `StateHalt` reached

#### Page Providers

The driver bridges the btree and pager layers through provider interfaces:

```go
type pagerProvider struct {
    pager    *pager.Pager
    nextPage uint32
}

// Implements btree.PageProvider
func (pp *pagerProvider) GetPageData(pgno uint32) ([]byte, error)
func (pp *pagerProvider) AllocatePageData() (uint32, []byte, error)
func (pp *pagerProvider) MarkDirty(pgno uint32) error
```

---

### btree Package

**Location**: `internal/btree/`

**Purpose**: Implements B+ tree data structure for efficient key-value storage with ordered access.

#### Architecture

```
B-tree Structure:
┌────────────────────────────────────────┐
│         Interior Page (Non-leaf)       │
│  ┌──────┬──────┬──────┬──────────┐    │
│  │ Ptr1 │ Key1 │ Ptr2 │ Key2 ... │    │
│  └──┬───┴──────┴───┬──┴──────────┘    │
└─────┼──────────────┼──────────────────┘
      │              │
      ▼              ▼
 ┌─────────┐    ┌─────────┐
 │  Leaf   │◄──►│  Leaf   │  (Doubly-linked)
 │  Page   │    │  Page   │
 └─────────┘    └─────────┘
   Contains       Contains
   data cells     data cells
```

#### Key Types

##### Btree
```go
type Btree struct {
    PageSize     uint32            // Bytes per page (typically 4096)
    UsableSize   uint32            // Usable bytes (pageSize - reserved)
    ReservedSize uint32            // Reserved bytes at page end
    Pages        map[uint32][]byte // In-memory page cache
    Provider     PageProvider      // Page storage backend
    mu           sync.RWMutex      // Thread safety
}
```

**Key Operations**:
- `GetPage(pgno)` - Retrieves page with validation
- `CreateTable()` - Allocates root page for new table
- `DropTable(rootPage)` - Recursively frees all pages
- `AllocatePage()` - Gets free page or extends database
- `NewRowid(rootPage)` - Generates unique rowid

##### PageHeader
```go
type PageHeader struct {
    PageType         byte   // 0x0D=leaf table, 0x05=interior table, etc.
    FirstFreeblock   uint16 // Offset to first freeblock
    NumCells         uint16 // Number of cells on page
    CellContentStart uint16 // Offset to first byte of cell content
    FragmentedBytes  byte   // Number of fragmented free bytes
    RightChild       uint32 // Right-most child page (interior only)
    IsInterior       bool   // True for interior pages
    CellPtrOffset    int    // Offset where cell pointer array starts
}
```

##### Cursor
```go
type Cursor struct {
    Btree    *Btree
    RootPage uint32
    PageNum  uint32   // Current page
    CellIdx  int      // Current cell index
    AtEOF    bool     // End of file marker
}
```

**Cursor Operations**:
- `MoveToFirst()` - Position at first entry
- `MoveToLast()` - Position at last entry
- `SeekGE(key)` - Seek to first entry >= key
- `Next()` - Advance to next entry
- `GetKey()` - Get rowid of current entry
- `GetData()` - Get data payload of current entry

#### Cell Structure

Cells store the actual data in B-tree pages:

**Table Leaf Cell** (stores user data):
```
[Payload size: varint]
[Rowid: varint]
[Payload: bytes]
[Overflow page number: 4 bytes if overflow]
```

**Index Leaf Cell** (stores index entries):
```
[Payload size: varint]
[Payload: bytes]
[Overflow page number: 4 bytes if overflow]
```

**Interior Cell** (points to child pages):
```
[Left child page: 4 bytes]
[Key: varint for tables, bytes for indexes]
```

#### Page Splitting and Merging

- **Split**: When a page is full, it splits into two pages
- **Merge**: When pages become too empty, they merge
- **Balance**: Redistributes cells between siblings for optimal space usage

---

### pager Package

**Location**: `internal/pager/`

**Purpose**: Manages page-level I/O, caching, journaling, and transaction support.

#### Architecture

```
Pager State Machine:
                                ┌─────────┐
                                │  OPEN   │
                                └────┬────┘
                                     │
                    ┌────────────────┼────────────────┐
                    │                                 │
            ┌───────▼────────┐              ┌────────▼─────────┐
            │    READER      │              │ WRITER_LOCKED    │
            │ (Shared lock)  │              │(Reserved lock)   │
            └────────────────┘              └────────┬─────────┘
                                                     │
                                            ┌────────▼──────────┐
                                            │ WRITER_CACHEMOD   │
                                            │(Cache dirty)      │
                                            └────────┬──────────┘
                                                     │
                                            ┌────────▼──────────┐
                                            │ WRITER_DBMOD      │
                                            │(Database dirty)   │
                                            └────────┬──────────┘
                                                     │
                                            ┌────────▼──────────┐
                                            │WRITER_FINISHED    │
                                            │(Ready to commit)  │
                                            └────────┬──────────┘
                                                     │
                                            ┌────────▼──────────┐
                                            │     ERROR         │
                                            └───────────────────┘
```

#### Key Types

##### Pager
```go
type Pager struct {
    file         *os.File
    journalFile  *os.File
    cache        PageCacheInterface
    header       *DatabaseHeader
    freeList     *FreeList
    state        int              // Pager state (OPEN, READER, WRITER_*, etc.)
    lockState    int              // Lock state (NONE, SHARED, RESERVED, etc.)
    pageSize     int
    dbSize       Pgno             // Current database size
    dbOrigSize   Pgno             // Size at transaction start
    journalMode  int              // DELETE, PERSIST, OFF, WAL
    readOnly     bool
    mu           sync.RWMutex
}
```

**Key Methods**:
- `Get(pgno)` - Retrieves page (from cache or disk)
- `Write(page)` - Marks page dirty, journals original
- `Commit()` - Writes dirty pages and finalizes journal
- `Rollback()` - Restores pages from journal
- `BeginWrite()` - Starts write transaction
- `AllocatePage()` - Allocates from freelist or extends DB

##### DbPage
```go
type DbPage struct {
    Pgno      Pgno         // Page number
    Data      []byte       // Page data
    dirty     bool         // Modified flag
    writeable bool         // Can be modified
    refCount  int32        // Reference count (atomic)
    pager     *Pager       // Back-reference
}
```

**Reference Counting**: Prevents premature eviction of in-use pages

##### DatabaseHeader
```go
type DatabaseHeader struct {
    Magic             [16]byte  // "SQLite format 3\x00"
    PageSize          uint16    // Power of 2 between 512-65536
    DatabaseSize      uint32    // Total pages in database
    FreelistTrunk     uint32    // First freelist trunk page
    FreelistCount     uint32    // Total free pages
    FileChangeCounter uint32    // Incremented on each write
    // ... 100 bytes total
}
```

#### Transaction Support

**Write Transaction Phases** (5-phase commit):
1. **Phase 0**: Flush freelist changes to disk
2. **Phase 1**: Write all dirty pages to database file
3. **Phase 2**: Sync database file to disk
4. **Phase 3**: Delete/truncate journal file
5. **Phase 4**: Update database header
6. **Phase 5**: Cleanup (clear dirty flags, reset state)

**Journal Format**:
```
[4 bytes: page size]
[4 bytes: page number]
[N bytes: original page data]
[4 bytes: CRC32 checksum]
... repeat for each journaled page ...
```

#### Caching

Two cache implementations:

**PageCache** (simple map-based):
```go
type PageCache struct {
    pages      map[Pgno]*DbPage
    dirtyPages map[Pgno]*DbPage
    maxPages   int
}
```

**LRUCache** (eviction-based):
```go
type LRUCache struct {
    capacity   int
    pages      map[Pgno]*lruEntry
    lruList    *list.List
    dirtyPages map[Pgno]*DbPage
    mode       CacheMode  // WriteThrough or WriteBack
}
```

#### WAL Mode (Write-Ahead Logging)

```
WAL File Structure:
┌─────────────────┐
│   WAL Header    │ (32 bytes)
├─────────────────┤
│   Frame 1       │ (24 byte header + page data)
├─────────────────┤
│   Frame 2       │
├─────────────────┤
│      ...        │
└─────────────────┘
```

**WAL Advantages**:
- Writers don't block readers
- Better concurrency
- Faster commits (no fsync on every transaction)

**Checkpointing**: Periodically merges WAL into main database

---

### vdbe Package

**Location**: `internal/vdbe/`

**Purpose**: Virtual Database Engine - a register-based virtual machine that executes bytecode programs compiled from SQL.

#### Architecture

```
VDBE Execution Model:

┌──────────────────────────────────────┐
│        VDBE Registers (Mem)          │
│  [0]  [1]  [2]  [3] ... [numRegs]   │
└──────────────────────────────────────┘
              ▲
              │ Read/Write
┌─────────────┴─────────────────────┐
│      Instruction Stream           │
│  [OpOpenRead 0, 1, 0]             │
│  [OpRewind 0, 8]                  │
│  [OpColumn 0, 1, 2]               │
│  [OpResultRow 2, 1]               │
│  [OpNext 0, 2]                    │
│  [OpHalt]                         │
└───────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│        Cursor Array                 │
│  Cursor[0] -> Table root=1          │
│  Cursor[1] -> Index root=3          │
└─────────────────────────────────────┘
```

#### Key Types

##### VDBE
```go
type VDBE struct {
    Program      []*Instruction
    PC           int              // Program counter
    Registers    []*Mem           // Register file
    Cursors      []*Cursor        // Open cursors
    ResultRow    []*Mem           // Current result row
    State        int              // Init, Ready, Run, RowReady, Halt
    NumSteps     int64            // Step counter
    LastInsertID int64            // Last INSERT rowid
    NumChanges   int64            // Rows modified
    AggCtx       map[string]interface{}  // Aggregate contexts
    Sorters      map[int]*Sorter  // Sorter instances
}
```

##### Instruction
```go
type Instruction struct {
    Opcode Opcode
    P1     int
    P2     int
    P3     int
    P4     interface{}  // Generic operand (string, []byte, etc.)
    P5     int
    Comment string      // For debugging
}
```

**Example Opcodes**:
- `OpInteger`: Load integer into register
- `OpOpenRead`: Open table/index for reading
- `OpRewind`: Move cursor to first entry
- `OpColumn`: Read column from current cursor position
- `OpResultRow`: Mark row ready for application
- `OpNext`: Advance cursor to next entry
- `OpInsert`: Insert data at cursor position
- `OpHalt`: Terminate program

##### Mem (Memory Cell)
```go
type Mem struct {
    flags    uint16       // Type flags (Int, Real, Str, Blob, Null)
    intValue int64
    realValue float64
    strValue  string
    blobValue []byte
}
```

**Type System**:
```go
const (
    MemNull  = 0x0001
    MemInt   = 0x0002
    MemReal  = 0x0004
    MemStr   = 0x0008
    MemBlob  = 0x0010
)
```

##### Cursor
```go
type Cursor struct {
    RootPage    uint32
    BtreeCursor *btree.Cursor
    Writable    bool
    SeekPos     int64
    Pseudo      bool         // For ephemeral tables
    PseudoData  []byte
}
```

#### Instruction Set Examples

**Simple SELECT** (`SELECT id, name FROM users`):
```
0: OpInit       0, 10, 0          # Jump to 10 if no transaction
1: OpOpenRead   0, 1, 2           # Open cursor 0 on table (root=1), 2 cols
2: OpRewind     0, 8, 0           # Rewind cursor 0, jump to 8 if empty
3: OpColumn     0, 0, 1           # Read col 0 to reg 1 (id)
4: OpColumn     0, 1, 2           # Read col 1 to reg 2 (name)
5: OpResultRow  1, 2, 0           # Return regs 1-2 as result
6: OpNext       0, 3, 0           # Next row, jump to 3
7: OpClose      0, 0, 0           # Close cursor 0
8: OpHalt       0, 0, 0           # Halt execution
```

**INSERT** (`INSERT INTO users VALUES (1, 'Alice')`):
```
0: OpTransaction 1, 0, 0          # Begin write transaction
1: OpInteger     1, 1, 0          # Load 1 into reg 1
2: OpString      0, 2, 0, 'Alice' # Load 'Alice' into reg 2
3: OpMakeRecord  1, 2, 3          # Make record from regs 1-2 into reg 3
4: OpOpenWrite   0, 1, 2          # Open cursor 0 for writing (root=1)
5: OpNewRowid    0, 4, 0          # Generate rowid into reg 4
6: OpInsert      0, 3, 4          # Insert record (reg 3) with rowid (reg 4)
7: OpClose       0, 0, 0          # Close cursor
8: OpHalt        0, 0, 0          # Halt
```

#### Execution Flow

```go
func (v *VDBE) Step() (bool, error) {
    // 1. Check state and transition to Run
    // 2. Fetch instruction at PC
    // 3. Increment PC
    // 4. Execute instruction via dispatch table
    // 5. Check state:
    //    - StateRowReady -> return true
    //    - StateHalt -> return false
    //    - StateRun -> continue loop
}
```

**Dispatch Table**: Maps opcodes to handler functions for O(1) dispatch

---

### parser Package

**Location**: `internal/parser/`

**Purpose**: Lexical analysis and parsing of SQL statements into Abstract Syntax Trees (AST).

#### Architecture

```
SQL Text → Lexer → Tokens → Parser → AST

Example:
"SELECT id FROM users WHERE age > 18"
           ↓ Lexer
[SELECT] [ID:id] [FROM] [ID:users] [WHERE] [ID:age] [>] [INTEGER:18]
           ↓ Parser
SelectStmt {
    Columns: [ResultColumn{Name: "id"}]
    From: [TableRef{Name: "users"}]
    Where: BinaryExpr{
        Left: Identifier{Name: "age"}
        Op: ">"
        Right: IntegerLiteral{Value: 18}
    }
}
```

#### Key Types

##### Parser
```go
type Parser struct {
    lexer     *Lexer
    tokens    []Token
    current   int
    errors    []string
    exprDepth int  // Prevents stack overflow in deep expressions
}
```

**Security Features**:
- Maximum SQL length: `security.MaxSQLLength` (1 MB)
- Maximum token count: `security.MaxTokens` (10,000)
- Maximum expression depth: `security.MaxExprDepth` (1,000)

##### Token
```go
type Token struct {
    Type    TokenType
    Literal string
    Line    int
    Column  int
}

type TokenType int
const (
    TK_SELECT = iota
    TK_FROM
    TK_WHERE
    // ... ~150 token types
)
```

##### AST Nodes

**Statement Interface**:
```go
type Statement interface {
    Node
    statementNode()
    String() string
}
```

**Key Statement Types**:
- `SelectStmt` - SELECT queries (with CTEs, joins, subqueries)
- `InsertStmt` - INSERT with conflict handling (REPLACE, IGNORE)
- `UpdateStmt` - UPDATE with WHERE
- `DeleteStmt` - DELETE with WHERE
- `CreateTableStmt` - Table creation with constraints
- `CreateIndexStmt` - Index creation
- `AlterTableStmt` - Table alteration
- `TransactionStmt` - BEGIN, COMMIT, ROLLBACK

**Expression Types**:
- `Identifier` - Column/table names
- `IntegerLiteral`, `FloatLiteral`, `StringLiteral`
- `BinaryExpr` - Binary operations (+, -, *, /, =, <, >, AND, OR, etc.)
- `UnaryExpr` - NOT, -, +
- `FunctionCall` - SQL function calls
- `CaseExpr` - CASE expressions
- `SubqueryExpr` - Subquery in expression

#### Parsing Strategy

**Recursive Descent**: Each grammar rule is a method

```go
func (p *Parser) parseSelect() (*SelectStmt, error) {
    stmt := &SelectStmt{}

    // Parse WITH clause (CTEs)
    if p.check(TK_WITH) {
        stmt.With = p.parseWithClause()
    }

    // Parse DISTINCT/ALL
    if p.match(TK_DISTINCT) {
        stmt.Distinct = true
    }

    // Parse result columns
    stmt.Columns = p.parseResultColumns()

    // Parse FROM clause
    if p.match(TK_FROM) {
        stmt.From = p.parseTableReferences()
    }

    // Parse WHERE
    if p.match(TK_WHERE) {
        stmt.Where = p.parseExpression()
    }

    // Parse GROUP BY
    if p.match(TK_GROUP, TK_BY) {
        stmt.GroupBy = p.parseExpressionList()
    }

    // Parse HAVING
    if p.match(TK_HAVING) {
        stmt.Having = p.parseExpression()
    }

    // Parse ORDER BY
    if p.match(TK_ORDER, TK_BY) {
        stmt.OrderBy = p.parseOrderingTerms()
    }

    // Parse LIMIT
    if p.match(TK_LIMIT) {
        stmt.Limit = p.parseExpression()
        if p.match(TK_OFFSET) {
            stmt.Offset = p.parseExpression()
        }
    }

    return stmt, nil
}
```

**Operator Precedence** (lowest to highest):
1. `OR`
2. `AND`
3. `NOT`
4. `=, <>, !=, <, <=, >, >=, IS, IN, LIKE, BETWEEN`
5. `||` (concatenation)
6. `+, -`
7. `*, /, %`
8. `~` (bitwise NOT)
9. Unary `+, -`

---

### schema Package

**Location**: `internal/schema/`

**Purpose**: Manages database schema metadata (tables, indexes, views, triggers) and type affinity rules.

#### Key Types

##### Schema
```go
type Schema struct {
    Tables    map[string]*Table
    Indexes   map[string]*Index
    Views     map[string]*View
    Triggers  map[string]*Trigger
    Sequences *SequenceManager  // AUTOINCREMENT support
    mu        sync.RWMutex
}
```

**Thread Safety**: All operations are protected by `sync.RWMutex`

##### Table
```go
type Table struct {
    Name         string
    RootPage     uint32           // B-tree root page
    SQL          string           // Original CREATE TABLE statement
    Columns      []*Column
    PrimaryKey   []string
    WithoutRowID bool
    Strict       bool             // STRICT mode (enforce types)
    Temp         bool             // Temporary table
    Constraints  []TableConstraint
}
```

##### Column
```go
type Column struct {
    Name     string
    Type     string               // Declared type (e.g., "INTEGER", "TEXT")
    Affinity Affinity             // Type affinity
    NotNull  bool
    Default  interface{}

    // Constraints
    PrimaryKey    bool
    Unique        bool
    Autoincrement bool
    Collation     string
    Check         string           // CHECK expression

    // Generated columns
    Generated       bool
    GeneratedExpr   string
    GeneratedStored bool          // STORED vs VIRTUAL
}
```

##### Affinity (Type System)
```go
type Affinity int

const (
    AffinityNone    Affinity = iota
    AffinityInteger          // INTEGER types
    AffinityText             // TEXT, CHAR, CLOB, VARCHAR
    AffinityReal             // REAL, FLOAT, DOUBLE
    AffinityNumeric          // NUMERIC, DECIMAL, BOOLEAN, DATE
    AffinityBlob             // BLOB, untyped
)
```

**Affinity Rules** (from SQLite):
1. If type contains "INT" → INTEGER affinity
2. If type contains "CHAR", "CLOB", "TEXT" → TEXT affinity
3. If type contains "BLOB" or no type → BLOB affinity
4. If type contains "REAL", "FLOA", "DOUB" → REAL affinity
5. Otherwise → NUMERIC affinity

##### Index
```go
type Index struct {
    Name     string
    Table    string           // Table name
    RootPage uint32
    SQL      string
    Columns  []string         // Indexed columns
    Unique   bool
    Partial  bool             // Has WHERE clause
    Where    string           // WHERE expression for partial indexes
}
```

#### Schema Loading

**sqlite_master Table** (system catalog):
```sql
CREATE TABLE sqlite_master (
    type TEXT,        -- 'table', 'index', 'view', 'trigger'
    name TEXT,        -- Object name
    tbl_name TEXT,    -- Associated table
    rootpage INTEGER, -- Root page in B-tree
    sql TEXT          -- CREATE statement
);
```

**Loading Process**:
1. Read page 1 (contains sqlite_master table)
2. Parse each row's SQL statement
3. Populate Schema object with Table/Index/View objects
4. Store rootpage mapping for B-tree access

---

### functions Package

**Location**: `internal/functions/`

**Purpose**: Implements SQL built-in functions (scalar, aggregate, window) and supports user-defined functions.

#### Architecture

```
Function Registry:
┌─────────────────────────────────────┐
│         Registry                    │
├─────────────────────────────────────┤
│  Built-in Functions:                │
│    - Scalar (upper, lower, length)  │
│    - Aggregate (sum, count, avg)    │
│    - Window (row_number, rank)      │
│    - Date/Time (date, datetime)     │
│    - Math (abs, round, random)      │
│    - JSON (json_extract, json_*)    │
├─────────────────────────────────────┤
│  User-Defined Functions:            │
│    - Overloading by arg count       │
│    - Priority over built-ins        │
└─────────────────────────────────────┘
```

#### Key Interfaces

##### Function
```go
type Function interface {
    Name() string
    NumArgs() int        // -1 for variadic
    Call(args []Value) (Value, error)
}
```

##### AggregateFunction
```go
type AggregateFunction interface {
    Function
    Step(args []Value) error      // Process one row
    Final() (Value, error)        // Return aggregate result
    Reset()                       // Reset for next group
}
```

**Example**: SUM aggregate
```go
type SumFunc struct {
    sum      float64
    hasValue bool
}

func (f *SumFunc) Step(args []Value) error {
    if !args[0].IsNull() {
        f.sum += args[0].AsFloat64()
        f.hasValue = true
    }
    return nil
}

func (f *SumFunc) Final() (Value, error) {
    if !f.hasValue {
        return NewNullValue(), nil
    }
    return NewFloatValue(f.sum), nil
}

func (f *SumFunc) Reset() {
    f.sum = 0
    f.hasValue = false
}
```

##### WindowFunction
```go
type WindowFunction interface {
    Function
    Inverse(args []Value) error   // Remove value (sliding windows)
    Value() (Value, error)        // Current window result
    Reset()
}
```

#### Value Interface

```go
type Value interface {
    Type() ValueType               // Null, Integer, Float, Text, Blob
    AsInt64() int64
    AsFloat64() float64
    AsString() string
    AsBlob() []byte
    IsNull() bool
    Bytes() int
}
```

#### Built-in Function Categories

**Scalar Functions**:
- String: `upper`, `lower`, `length`, `substr`, `trim`, `replace`, `printf`
- Math: `abs`, `round`, `ceil`, `floor`, `sqrt`, `power`, `random`
- Type: `typeof`, `cast`
- Utility: `coalesce`, `ifnull`, `nullif`, `hex`, `quote`

**Aggregate Functions**:
- Basic: `count`, `sum`, `avg`, `min`, `max`
- Statistical: `total`, `group_concat`
- Advanced: `count(DISTINCT)`

**Window Functions**:
- Ranking: `row_number`, `rank`, `dense_rank`, `percent_rank`
- Distribution: `ntile`, `cume_dist`
- Value: `first_value`, `last_value`, `nth_value`, `lag`, `lead`

**Date/Time Functions**:
- `date`, `time`, `datetime`, `julianday`, `strftime`
- Modifiers: `'+1 day'`, `'start of month'`, etc.

**JSON Functions**:
- `json`, `json_array`, `json_object`
- `json_extract`, `json_set`, `json_insert`, `json_remove`
- `json_type`, `json_valid`, `json_each`, `json_tree`

#### Function Registration

**User-Defined Functions**:
```go
// Register scalar function
config := functions.FunctionConfig{
    Name:          "double",
    NumArgs:       1,
    Deterministic: true,
}
functions.RegisterScalarFunction(registry, config, &DoubleFunc{})

// Register aggregate function
config := functions.FunctionConfig{
    Name:          "product",
    NumArgs:       1,
    Deterministic: true,
}
functions.RegisterAggregateFunction(registry, config, &ProductFunc{})
```

**Overloading**: Functions can be overloaded by argument count
```go
// substr(str, start) - 2 args
// substr(str, start, length) - 3 args
registry.RegisterUser(substr2Func, 2)
registry.RegisterUser(substr3Func, 3)
```

---

### security Package

**Location**: `internal/security/`

**Purpose**: Provides security controls including path validation, arithmetic overflow protection, and resource limits.

#### Path Validation (4-Layer Model)

```
Layer 1: Character Validation
├─ Block null bytes (\x00)
├─ Block control characters (0x00-0x1F)
└─ Block path traversal patterns (..)

Layer 2: Sandbox Resolution
├─ Resolve path within DatabaseRoot
├─ Verify prefix match
└─ Prevent escape via symlinks/..

Layer 3: Allowlist Checking
├─ Check AllowedSubdirs if configured
└─ Reject paths outside allowed dirs

Layer 4: Symlink Detection
├─ Check if path is symlink
├─ Check parent directories
└─ Block symlinks if configured
```

##### SecurityConfig
```go
type SecurityConfig struct {
    // Layer 1
    BlockNullBytes     bool
    BlockTraversal     bool
    BlockAbsolutePaths bool

    // Layer 2
    EnforceSandbox     bool
    DatabaseRoot       string

    // Layer 3
    AllowedSubdirs     []string

    // Layer 4
    BlockSymlinks      bool
}
```

**Usage**:
```go
config := security.DefaultSecurityConfig()
config.DatabaseRoot = "/var/lib/myapp/databases"
config.AllowedSubdirs = []string{"users", "analytics"}

validPath, err := security.ValidateDatabasePath(userInput, config)
```

#### Resource Limits

```go
const (
    MaxSQLLength   = 1_048_576    // 1 MB
    MaxTokens      = 10_000       // Max tokens in SQL
    MaxExprDepth   = 1_000        // Max expression nesting
    MaxTableDepth  = 64           // Max nested tables/subqueries
    MaxCompoundSelect = 500       // Max UNION terms
)
```

#### Arithmetic Safety

**Checked Operations** (prevent overflow):
```go
// Returns (result, overflow)
func AddInt64Checked(a, b int64) (int64, bool)
func SubInt64Checked(a, b int64) (int64, bool)
func MulInt64Checked(a, b int64) (int64, bool)
```

**Division Safety**:
```go
func DivInt64Safe(a, b int64) (int64, error)
// Returns error on division by zero or INT64_MIN / -1
```

---

## Supporting Packages

### expr Package

**Purpose**: Expression evaluation, type conversion, and comparison logic

**Key Features**:
- Type affinity application
- Collation-aware string comparison
- Arithmetic with overflow detection
- Expression compilation to VDBE code

### planner Package

**Purpose**: Query planning and optimization

**Key Components**:
- `QueryPlan` - Execution plan with cost estimation
- `WhereLoop` - WHERE clause optimization
- `JoinOptimizer` - Join order selection
- `IndexSelector` - Index usage decisions
- `CTEResolver` - Common Table Expression handling

**Optimization Techniques**:
- Index selection based on WHERE clause
- Join order optimization (cost-based)
- Predicate pushdown
- Constant folding
- Subquery flattening

### engine Package

**Purpose**: Compiles AST to VDBE bytecode

**Key Components**:
- `Compiler` - Main compilation engine
- `CodeGenerator` - VDBE instruction emission
- `ExprCompiler` - Expression → bytecode
- `TriggerCompiler` - Trigger compilation

**Compilation Flow**:
```
AST → Planner → Plan → Compiler → VDBE Program
```

### collation Package

**Purpose**: String collation (sorting/comparison) support

**Collations**:
- `BINARY` - Byte-by-byte comparison
- `NOCASE` - Case-insensitive ASCII
- `RTRIM` - Trailing space trimming

### constraint Package

**Purpose**: Constraint validation (NOT NULL, CHECK, UNIQUE, FK)

**Enforcement Points**:
- INSERT: Validate all constraints
- UPDATE: Validate modified columns
- DELETE: Check foreign key references

### vtab Package

**Purpose**: Virtual table interface

**Built-in Virtual Tables**:
- `sqlite_master` - System catalog
- `pragma_*` - PRAGMA interface
- `fts5` - Full-text search
- `rtree` - R-tree spatial indexing

---

## Data Flow

### SELECT Query Execution

```
1. SQL Text
   │
   ▼
2. Parser → AST (SelectStmt)
   │
   ▼
3. Planner → QueryPlan
   │  - Analyze WHERE clause
   │  - Select indexes
   │  - Determine join order
   │
   ▼
4. Engine → VDBE Program
   │  - Emit OpOpenRead for tables/indexes
   │  - Emit OpRewind to start iteration
   │  - Emit OpColumn to read columns
   │  - Emit WHERE filtering (OpEq, OpLt, etc.)
   │  - Emit OpResultRow for each result
   │  - Emit OpNext for iteration
   │  - Emit OpHalt when done
   │
   ▼
5. VDBE Execution
   │  - Step through program
   │  - Use Cursor to scan B-tree
   │  - Read pages via Pager
   │  - Return rows to driver
   │
   ▼
6. driver.Rows → application
```

### INSERT Statement Execution

```
1. SQL: INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')
   │
   ▼
2. Parser → InsertStmt
   │
   ▼
3. Engine compiles to VDBE:
   OpTransaction    - Begin write transaction
   OpString 'Alice' → reg[1]
   OpString 'alice@example.com' → reg[2]
   OpMakeRecord reg[1..2] → reg[3]
   OpOpenWrite cursor[0], rootPage=1
   OpNewRowid cursor[0] → reg[4]
   OpInsert cursor[0], reg[3], reg[4]
   OpHalt
   │
   ▼
4. VDBE executes:
   - Pager begins write transaction
   - Pager journals affected pages
   - Btree allocates new cell
   - Btree inserts data
   - Pager marks pages dirty
   │
   ▼
5. Auto-commit (if not in explicit transaction):
   - Pager writes dirty pages
   - Pager syncs database file
   - Pager deletes journal
   - Pager updates header
```

---

## Transaction Architecture

### ACID Compliance

**Atomicity**: Rollback journal ensures all-or-nothing commits

**Consistency**: Constraints enforced in VDBE

**Isolation**: File locking provides serializable isolation
- Shared locks for readers (multiple readers allowed)
- Exclusive lock for writer (blocks all others)

**Durability**: fsync ensures data persisted to disk

### Lock Progression

```
NONE → SHARED → RESERVED → PENDING → EXCLUSIVE

NONE:      No access
SHARED:    Can read (multiple readers)
RESERVED:  Intent to write (can coexist with readers)
PENDING:   Waiting for readers to finish
EXCLUSIVE: Writing (no readers/writers)
```

**Read Transaction**:
```
NONE → SHARED (read data) → NONE
```

**Write Transaction**:
```
NONE → SHARED (read) → RESERVED (write intent) →
EXCLUSIVE (write) → NONE (commit/rollback)
```

### Savepoints (Nested Transactions)

```sql
BEGIN;
  INSERT INTO users VALUES (1, 'Alice');
  SAVEPOINT sp1;
    UPDATE users SET name = 'Bob' WHERE id = 1;
    SAVEPOINT sp2;
      DELETE FROM users WHERE id = 1;
    ROLLBACK TO sp2;  -- Undo DELETE
  ROLLBACK TO sp1;    -- Undo UPDATE
  INSERT INTO users VALUES (2, 'Charlie');
COMMIT;
-- Result: Alice and Charlie inserted, Bob never existed
```

**Implementation**:
- Each savepoint journals pages at that point
- Rollback restores from savepoint journal
- Commit discards savepoint journals

---

## Extension Points

### User-Defined Functions

```go
// Scalar function
type MyFunc struct{}
func (f *MyFunc) Invoke(args []functions.Value) (functions.Value, error) {
    // Custom logic
    return functions.NewIntValue(result), nil
}

conn.CreateScalarFunction("my_func", 1, true, &MyFunc{})
```

### Virtual Tables

```go
type MyVTab struct {
    vtab.Module
}

func (m *MyVTab) Create(db *vtab.Database, args []string) (vtab.Table, error) {
    // Return custom table implementation
}

// Register module
vtab.RegisterModule("my_vtab", &MyVTab{})
```

### Custom Collations

```go
type MyCollation struct{}

func (c *MyCollation) Compare(a, b string) int {
    // Custom comparison logic
    return strings.Compare(normalize(a), normalize(b))
}

collation.Register("my_collation", &MyCollation{})
```

### Hooks and Callbacks

**Busy Handler** (for lock contention):
```go
pager.SetBusyHandler(func(retries int) bool {
    if retries > 10 {
        return false  // Give up
    }
    time.Sleep(100 * time.Millisecond)
    return true  // Retry
})
```

**Progress Handler** (periodic callback):
```go
vdbe.SetProgressHandler(1000, func() bool {
    // Called every 1000 VDBE instructions
    if shouldCancel {
        return false  // Abort execution
    }
    return true  // Continue
})
```

---

## Best Practices for Contributors

### Thread Safety

1. **Always lock** when accessing shared state
2. **Lock ordering** to prevent deadlock:
   - Driver.mu before Conn.mu
   - Conn.mu before Stmt.mu
   - Never acquire in reverse order
3. **RWMutex** for read-heavy data (Schema, Pager cache)

### Error Handling

1. **Wrap errors** with context:
   ```go
   return fmt.Errorf("failed to open page %d: %w", pgno, err)
   ```
2. **Rollback** on error in write operations
3. **Return early** to reduce nesting

### Testing

1. **Unit tests** for each package
2. **Integration tests** at driver level
3. **SQL Logic Tests** (SLT) for correctness
4. **Fuzz testing** for parser and security

### Performance

1. **Minimize allocations** in hot paths (use object pools)
2. **Cache lookups** (page cache, schema cache)
3. **Batch operations** where possible
4. **Profile before optimizing** (use pprof)

### Security

1. **Validate all inputs** (SQL, file paths)
2. **Enforce resource limits** (SQL length, expression depth)
3. **Prevent overflow** in arithmetic
4. **Sanitize errors** (don't leak internal paths)

---

## Debugging Tips

### EXPLAIN Query Plans

```sql
EXPLAIN QUERY PLAN SELECT * FROM users WHERE id = 1;
-- Shows table scans vs index usage
```

### VDBE Disassembly

```sql
EXPLAIN SELECT * FROM users WHERE id = 1;
-- Shows VDBE bytecode program
```

### Enable Logging

Set environment variable:
```bash
export ANTHONY_DEBUG=1
```

### Tracing

```go
vdbe.SetTraceCallback(func(v *vdbe.VDBE, instr *vdbe.Instruction) {
    fmt.Printf("PC=%d %s\n", v.PC, instr.String())
})
```

### Integrity Check

```sql
PRAGMA integrity_check;
-- Verifies database structure
```

---

## Related Documentation

- [User API Documentation](USER_API.md) - Public API for applications
- [SQL Reference](SQL_REFERENCE.md) - Supported SQL syntax
- [ARCHITECTURE.md](../ARCHITECTURE.md) - High-level architecture overview
- [CONTRIBUTING.md](../CONTRIBUTING.md) - Contribution guidelines

---

## Version Information

**Document Version**: 1.0
**Anthony Version**: 0.1.0
**Last Updated**: 2026-02-28

---

## License

This documentation is part of the Anthony SQLite project and is licensed under the same terms as the source code.
