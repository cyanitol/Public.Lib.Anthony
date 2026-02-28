# Anthony SQLite - Architecture Overview

This document describes the high-level architecture of the Anthony pure Go SQLite implementation.

## Design Philosophy

Anthony is a from-scratch implementation of SQLite in pure Go, following these principles:

1. **Pure Go**: No CGO dependencies, fully portable
2. **SQLite Compatible**: Follows SQLite file format and SQL dialect
3. **Clean Architecture**: Layered design with clear separation of concerns
4. **Production Quality**: Comprehensive testing, error handling, and documentation
5. **Public Domain**: Same license as SQLite

## Package Overview

| Package | Purpose | Key Responsibilities |
|---------|---------|---------------------|
| `driver` | database/sql interface | Connection management, statement execution, type conversion |
| `engine` | Top-level integration | Coordinates all components, SQL compilation, transaction management |
| `parser` | SQL parsing | Lexical analysis, syntax parsing, AST construction |
| `planner` | Query optimization | Join ordering, index selection, CTE planning, cost estimation |
| `sql` | SQL compilation | Compiles AST to VDBE bytecode (SELECT/INSERT/UPDATE/DELETE/DDL) |
| `vdbe` | Execution engine | Register-based bytecode VM, cursor operations, expression evaluation |
| `btree` | B-tree storage | Tree navigation, cell insertion/deletion, page splitting |
| `pager` | Page management | Page cache, journaling, WAL, locks, transaction coordination |
| `schema` | Schema tracking | Table/column/index definitions, type affinity, sqlite_master |
| `constraint` | Constraint enforcement | CHECK/UNIQUE validation, collation, backing indexes |
| `expr` | Expression compilation | Expression tree traversal, type coercion, operator code generation |
| `functions` | Built-in functions | Scalar functions (string, math, date/time) and aggregates |
| `security` | Security controls | Path validation, overflow detection, resource limits, sandboxing |
| `utf` | UTF encoding | UTF-8/UTF-16 conversion, varint encoding, collation support |
| `collation` | String collation | Collation sequences for comparison and sorting |
| `vtab` | Virtual tables | Virtual table modules, lifecycle, query planning, cursors |
| `format` | File format | Database header, page types, format validation |

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Application                             │
└──────────────┬──────────────────────────────────────────────┘
               │ database/sql interface
┌──────────────▼──────────────────────────────────────────────┐
│                    Driver Layer                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Connection, Statement, Transaction Management        │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────┬──────────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────┐
│                    Engine Layer                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Top-level integration, coordinates all components    │  │
│  │  (Pager, Btree, Schema, Parser, VDBE, Functions)     │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────┬──────────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────┐
│                  Query Processing Layer                      │
│  ┌────────────┐  ┌──────────┐  ┌──────────────────────┐   │
│  │   Parser   │─▶│ Planner  │─▶│ SQL Compiler         │   │
│  │            │  │          │  │                      │   │
│  │  Lexer     │  │ Optimizer│  │ SELECT/INSERT/       │   │
│  │  AST       │  │ CTE      │  │ UPDATE/DELETE/DDL    │   │
│  └────────────┘  └──────────┘  └──────────────────────┘   │
└──────────────┬──────────────────────────────────────────────┘
               │ VDBE bytecode
┌──────────────▼──────────────────────────────────────────────┐
│                 Execution Engine (VDBE)                      │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Virtual Machine with Register-Based Execution        │  │
│  │                                                        │  │
│  │  ┌──────────┐  ┌───────────┐  ┌────────────────┐   │  │
│  │  │ Memory   │  │ Expression│  │ Built-in       │   │  │
│  │  │ Cells    │  │ Evaluator │  │ Functions      │   │  │
│  │  └──────────┘  └───────────┘  └────────────────┘   │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────┬──────────────────────────────────────────────┘
               │ cursor operations
┌──────────────▼──────────────────────────────────────────────┐
│                    Storage Layer                             │
│  ┌────────────────────────────────────────────────────────┐│
│  │                B-Tree Engine                            ││
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────────┐    ││
│  │  │ Cursor   │  │ Cell     │  │ Page Split       │    ││
│  │  │ Navigation│  │ Parsing  │  │ Tree Growth      │    ││
│  │  └──────────┘  └──────────┘  └──────────────────┘    ││
│  └────────────────────────────────────────────────────────┘│
│  ┌────────────────────────────────────────────────────────┐│
│  │                 Pager                                   ││
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────────┐    ││
│  │  │ Page     │  │ Journal  │  │ WAL (Write-Ahead││    ││
│  │  │ Cache    │  │          │  │ Logging)         │    ││
│  │  └──────────┘  └──────────┘  └──────────────────┘    ││
│  └────────────────────────────────────────────────────────┘│
│  ┌────────────────────────────────────────────────────────┐│
│  │               File I/O                                  ││
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────────┐    ││
│  │  │ Database │  │ Lock     │  │ Format           │    ││
│  │  │ File     │  │ Manager  │  │ Validation       │    ││
│  │  └──────────┘  └──────────┘  └──────────────────┘    ││
│  └────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘

                   Cross-Cutting Concerns
┌─────────────────────────────────────────────────────────────┐
│  Security  │  UTF Encoding  │  Collation  │  Virtual Tables │
└─────────────────────────────────────────────────────────────┘
```

## Layer Descriptions

### 1. Driver Layer (`internal/driver`)

**Purpose**: Provides the Go database/sql compatible interface

**Responsibilities**:
- Connection lifecycle management
- Statement preparation and execution
- Transaction begin/commit/rollback
- Result set iteration
- Type conversions between Go and SQL types

**Key Types**:
- `Driver`: Implements database/sql/driver.Driver
- `Conn`: Database connection
- `Stmt`: Prepared statement
- `Rows`: Result set iterator

### 2. Engine Layer (`internal/engine`)

**Purpose**: Top-level integration layer that coordinates all database components

**Responsibilities**:
- Database lifecycle (open, close)
- Component integration (pager, btree, schema, parser, VDBE)
- SQL compilation coordination
- Transaction management
- Trigger execution
- Query result management

**Key Types**:
- `Engine`: Main database engine coordinating all subsystems
- `Compiler`: SQL-to-VDBE bytecode compiler
- `Result`: Query execution results

### 3. Query Processing Layer

#### Parser (`internal/parser`)
**Purpose**: Converts SQL text into Abstract Syntax Tree (AST)

**Responsibilities**:
- Lexical analysis (tokenization)
- Syntax parsing
- AST construction
- Syntax validation

**Key Types**:
- `Lexer`: Tokenizes SQL text
- `Parser`: Builds AST from tokens
- `SelectStmt`, `InsertStmt`, etc.: AST node types

#### Planner (`internal/planner`)
**Purpose**: Optimizes queries and generates execution plans

**Responsibilities**:
- Query optimization
- Join ordering
- Index selection
- CTE (Common Table Expression) planning
- Cost estimation

**Key Types**:
- `QueryPlan`: Execution plan
- `CTEPlanner`: CTE optimization
- `JoinOptimizer`: Join reordering

#### SQL Compiler (`internal/sql`)
**Purpose**: Compiles AST into VDBE bytecode

**Responsibilities**:
- SELECT compilation (including CTEs, subqueries, compound queries)
- INSERT/UPDATE/DELETE compilation
- DDL compilation (CREATE TABLE, DROP TABLE, ALTER TABLE)
- Constraint code generation

**Key Types**:
- `SelectCompiler`: Compiles SELECT statements
- `InsertCompiler`: Compiles INSERT statements
- `DDLCompiler`: Compiles DDL statements

### 4. Execution Engine (`internal/vdbe`)

**Purpose**: Executes VDBE bytecode programs

**Responsibilities**:
- Register-based bytecode execution
- Memory cell management
- Cursor operations
- Expression evaluation
- Function execution

**Key Types**:
- `VDBE`: Virtual machine instance
- `Mem`: Memory cell (can hold any SQL type)
- `Instruction`: Bytecode instruction
- `Cursor`: Points to rows in tables/indexes

**Instruction Categories**:
- Control flow (Goto, If, IfNot, Halt)
- Data operations (Integer, Real, String, Blob, Copy, Move)
- Cursor operations (OpenRead, OpenWrite, Rewind, Next, SeekGE)
- Data retrieval (Column, Rowid, ResultRow)
- Data modification (Insert, Delete, Update)
- Arithmetic (Add, Subtract, Multiply, Divide)
- Comparisons (Eq, Lt, Gt, Le, Ge, Ne)
- Functions (Function, AggStep, AggFinal)

### 5. Storage Layer

#### B-Tree Engine (`internal/btree`)
**Purpose**: Manages B-tree data structures on pages

**Responsibilities**:
- B-tree page management
- Cell insertion and deletion
- Tree navigation (cursors)
- Page splitting when full
- Integrity checking

**Key Types**:
- `Btree`: B-tree instance
- `BtCursor`: B-tree cursor for navigation
- `PageHeader`: Page metadata
- `CellInfo`: Cell data and metadata

**B-Tree Types**:
- Table B-Trees: Integer keys (rowids), data payloads
- Index B-Trees: Arbitrary keys, no separate data

#### Pager (`internal/pager`)
**Purpose**: Page-level I/O and transaction management

**Responsibilities**:
- Page caching
- Page I/O (read/write)
- Journal management
- WAL (Write-Ahead Logging)
- Lock management
- Transaction coordination

**Key Types**:
- `Pager`: Page manager instance
- `Page`: In-memory page
- `Journal`: Rollback journal
- `WAL`: Write-Ahead Log

### 6. Supporting Subsystems

#### Schema Management (`internal/schema`)
**Purpose**: Tracks database schema

**Responsibilities**:
- Table and column definitions
- Index definitions
- Type affinity rules
- sqlite_master table management

**Key Types**:
- `Schema`: Schema container
- `Table`: Table definition
- `Column`: Column definition
- `Index`: Index definition

#### Constraint Enforcement (`internal/constraint`)
**Purpose**: Enforces SQL constraints

**Responsibilities**:
- CHECK constraint validation
- UNIQUE constraint validation
- Collation sequence management
- Backing index creation

**Key Types**:
- `CheckValidator`: CHECK constraint validator
- `UniqueConstraint`: UNIQUE constraint
- `CollationFunc`: Collation function

#### Expression Evaluation (`internal/expr`)
**Purpose**: Compiles SQL expressions to VDBE bytecode

**Responsibilities**:
- Expression tree traversal
- Type checking and coercion
- Operator code generation
- Function call compilation

**Key Types**:
- `CodeGenerator`: Expression compiler
- `ExprContext`: Compilation context

#### Built-in Functions (`internal/functions`)
**Purpose**: Implements SQL built-in functions

**Responsibilities**:
- Scalar functions (string, math, date/time, type)
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT)
- Function registry

**Key Functions**:
- String: UPPER, LOWER, LENGTH, SUBSTR, TRIM, REPLACE
- Math: ABS, ROUND, SQRT, POWER, LOG, SIN, COS
- Date/Time: DATE, TIME, DATETIME, JULIANDAY, STRFTIME
- Aggregate: COUNT, SUM, AVG, MIN, MAX

#### File Format (`internal/format`)
**Purpose**: SQLite file format definitions

**Responsibilities**:
- Database header structure
- Page type constants
- Format validation
- Encoding specifications

#### Security (`internal/security`)
**Purpose**: Security controls and input validation

**Responsibilities**:
- Path validation and sanitization (preventing directory traversal)
- Null byte injection prevention
- Symlink attack prevention
- Arithmetic overflow detection
- Resource limits enforcement
- Database root sandboxing

**Key Types**:
- `SecurityConfig`: Security configuration settings
- Path validation functions
- Safe arithmetic operations

#### UTF Encoding (`internal/utf`)
**Purpose**: UTF-8 and UTF-16 encoding utilities

**Responsibilities**:
- UTF-8 string operations
- UTF-16 conversion (LE and BE)
- Variable-length integer encoding (varint)
- Character validation and normalization
- Collation sequence support

**Key Functions**:
- UTF-8/UTF-16 conversion
- Varint encode/decode
- String comparison with collation

#### Collation (`internal/collation`)
**Purpose**: String collation sequences for comparison and sorting

**Responsibilities**:
- Collation function definitions
- Custom collation registration
- Binary, nocase, and rtrim collations
- Locale-aware string comparison

**Key Types**:
- `CollationFunc`: Collation function signature
- Built-in collation implementations

#### Virtual Tables (`internal/vtab`)
**Purpose**: Virtual table module support

**Responsibilities**:
- Virtual table module registration
- Virtual table lifecycle (create, connect, destroy)
- Query planning for virtual tables (BestIndex)
- Virtual table cursors and iteration
- Virtual table updates (INSERT/UPDATE/DELETE)

**Key Types**:
- `Module`: Virtual table module interface
- `VirtualTable`: Virtual table instance
- `VirtualCursor`: Cursor for virtual table iteration
- `IndexInfo`: Query constraint and ordering information

## Data Flow

### Query Execution (SELECT)

1. **Driver**: Application calls database/sql interface
2. **Engine**: Receives SQL text, coordinates processing
3. **Parse**: SQL text → AST (Abstract Syntax Tree)
4. **Plan**: AST → Query Plan (optimized)
5. **Compile**: Query Plan → VDBE bytecode (via Engine.Compiler)
6. **Execute**: VDBE bytecode runs, produces rows
7. **Return**: Rows returned to application via driver

Example:
```sql
SELECT name, age FROM users WHERE age > 21 ORDER BY name;
```

Flow:
```
Application (database/sql)
  ↓
Driver Layer
  ↓
Engine.Execute(sql)
  ↓
Parser.Parse(sql)
  ↓
SelectStmt AST
  ↓
Planner.Optimize(ast)
  ↓
Query Plan (index scan on age, sort by name)
  ↓
Engine.Compiler.Compile(plan)
  ↓
VDBE Program:
  OpenRead cursor 0 on users
  Rewind cursor 0
  loop:
    Column 0, 1 → reg[1]  (age)
    Integer 21 → reg[2]
    Gt reg[1], reg[2], skip
    Column 0, 0 → reg[3]  (name)
    Column 0, 1 → reg[4]  (age)
    ResultRow reg[3], 2
  skip:
    Next cursor 0, loop
  Halt
  ↓
VDBE.Execute()
  ↓
Rows: [("Alice", 22), ("Bob", 25), ...]
  ↓
Result returned to Driver
  ↓
Application receives rows
```

### Data Modification (INSERT)

1. **Parse**: INSERT statement → AST
2. **Compile**: AST → VDBE bytecode (includes constraint checks)
3. **Execute**: VDBE inserts row into B-tree, updates indexes
4. **Journal**: Changes logged for rollback
5. **Commit**: Changes become permanent

### Transaction Processing

```
BEGIN
  ↓
Acquire Locks
  ↓
Execute Statements (with journaling)
  ↓
COMMIT → Flush journal, release locks
   or
ROLLBACK → Restore from journal, release locks
```

## Component Interactions

### Driver → Engine → VDBE Flow
- **Driver** provides the database/sql interface and manages connections
- **Engine** acts as the central coordinator, managing all subsystems
- Requests flow: Driver → Engine → Parser/Compiler → VDBE → Btree → Pager

### Schema and Metadata
- **Schema** package maintains table/index definitions in memory
- Synchronized with sqlite_master table in the database
- Used by Parser, Compiler, and VDBE for query validation and execution

### Security Integration
- **Path validation** occurs at Driver and Engine levels before file operations
- **Arithmetic overflow** checks in VDBE during expression evaluation
- **Resource limits** enforced in Pager (cache size, transaction size)
- **Input sanitization** in Parser (SQL injection prevention via prepared statements)

### Encoding and Collation
- **UTF encoding** used throughout for text storage and retrieval
- **Collation** functions applied during:
  - String comparisons in VDBE
  - ORDER BY operations
  - Index key comparisons in Btree
  - UNIQUE constraint validation

### Virtual Tables
- Virtual tables integrate with the query engine like regular tables
- **BestIndex** optimization allows virtual tables to influence query plans
- Virtual table cursors work alongside Btree cursors in VDBE

## Key Design Decisions

### Why VDBE (Virtual Machine)?

- **Flexibility**: Bytecode can be optimized, reordered, cached
- **Separation**: Clean separation between compilation and execution
- **Debugging**: Bytecode can be inspected (EXPLAIN)
- **Compatibility**: Matches SQLite's architecture

### Why B-Trees?

- **Efficient**: O(log n) search, insert, delete
- **Sequential Access**: Fast table scans
- **Disk-Friendly**: Matches page-based I/O
- **Proven**: Well-understood algorithm

### Why Pure Go?

- **Portability**: Works on any platform Go supports
- **Safety**: Memory safety, no buffer overflows
- **Concurrency**: Go's goroutines and channels
- **Tooling**: Go's excellent testing and profiling tools

## Concurrency Model

### Lock Hierarchy

Anthony implements a strict lock ordering to prevent deadlocks (see LOCK_ORDERING.md):

1. **Pager locks** (file-level, acquired first)
2. **Schema locks** (schema modifications)
3. **Btree locks** (tree structure modifications)
4. **Page locks** (individual page access)

### Transaction Isolation

- **Read Uncommitted**: Not supported (minimum is Read Committed)
- **Read Committed**: Default isolation level
- **Serializable**: Available via explicit locking

### Concurrent Access

- **Multiple Readers**: Supported simultaneously
- **Single Writer**: Exclusive write access during modifications
- **WAL Mode**: Allows readers during write transactions
- **Lock Escalation**: Automatic upgrade from shared to exclusive when needed

## Performance Considerations

### Optimization Strategies

1. **Indexing**: Automatic index use for WHERE clauses
2. **Query Planning**: Cost-based optimizer selects best plan
3. **Page Caching**: Frequently-used pages kept in memory
4. **Write-Ahead Logging**: Better concurrency, faster writes
5. **Connection Pooling**: Reuse connections (via database/sql)
6. **Prepared Statements**: Parse once, execute many times
7. **Index Selection**: Planner chooses optimal index for queries

### Bottlenecks

- **Disk I/O**: Mitigated by page cache and WAL
- **Lock Contention**: Mitigated by reader-writer locks and WAL mode
- **Memory Allocation**: Minimized in hot paths
- **Expression Evaluation**: Compiled to efficient bytecode
- **Large Transactions**: May require journal flushing

## Security Architecture

### Defense in Depth

Anthony implements multiple layers of security controls:

1. **Input Validation**
   - Path sanitization (null bytes, directory traversal, symlinks)
   - SQL injection prevention via prepared statements
   - Resource limit enforcement

2. **Memory Safety**
   - Pure Go implementation eliminates buffer overflows
   - Bounds checking on all array/slice access
   - Safe arithmetic operations with overflow detection

3. **Sandboxing**
   - Database root directory enforcement
   - Restricted file system access
   - Configurable allowed subdirectories

4. **Transaction Integrity**
   - ACID compliance via journaling and WAL
   - Lock ordering to prevent deadlocks (see LOCK_ORDERING.md)
   - Atomic commits with rollback on failure

5. **Type Safety**
   - Strong typing throughout the stack
   - Type affinity rules enforced
   - Runtime type validation

## Testing Strategy

### Unit Tests
- Each package has comprehensive unit tests
- Test coverage > 80% for critical paths
- Edge cases and error conditions covered
- Security-focused attack simulations

### Integration Tests
- End-to-end query execution tests
- Transaction isolation tests
- Constraint enforcement tests
- Virtual table integration tests

### Compatibility Tests
- SQLite test suite adaptation (where applicable)
- File format compatibility verification

### Security Tests
- Path traversal attack prevention
- Integer overflow detection
- Resource exhaustion protection
- Injection attack resistance

## Current Features

### Implemented
- ✓ Core SQL operations (SELECT, INSERT, UPDATE, DELETE)
- ✓ DDL operations (CREATE TABLE, DROP TABLE, ALTER TABLE)
- ✓ Common Table Expressions (CTEs) including recursive CTEs
- ✓ Compound SELECT (UNION, INTERSECT, EXCEPT)
- ✓ Subqueries (scalar, EXISTS, IN)
- ✓ Triggers (BEFORE/AFTER, FOR EACH ROW)
- ✓ CHECK and UNIQUE constraints
- ✓ Virtual table infrastructure
- ✓ Aggregate functions (COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT)
- ✓ String, math, and date/time functions
- ✓ Transaction support with journaling
- ✓ Write-Ahead Logging (WAL)
- ✓ Security controls (path validation, overflow detection)
- ✓ UTF-8/UTF-16 encoding support
- ✓ Collation sequences

### In Progress
- ATTACH/DETACH database support
- VACUUM optimization
- Enhanced query optimization

## Future Directions

### Short Term
- Complete FOREIGN KEY support with cascading actions
- Full-text search (FTS5) virtual table module
- Performance profiling and optimization
- Query plan caching

### Medium Term
- Concurrent query execution (parallel scans)
- Query result caching
- Better statistics for query planner (ANALYZE improvements)
- Incremental VACUUM

### Long Term
- Distributed/replicated databases
- Pluggable storage engines
- Custom function modules via plugins
- Advanced optimization (predicate pushdown, join reordering)

## Common Patterns and Best Practices

### Using the Engine

```go
// Open a database
engine, err := engine.Open("mydb.sqlite")
if err != nil {
    log.Fatal(err)
}
defer engine.Close()

// Execute a query
result, err := engine.Execute("SELECT * FROM users WHERE age > 18")
if err != nil {
    log.Fatal(err)
}
```

### Transaction Patterns

```go
// Begin transaction
engine.BeginTransaction()

// Execute multiple statements
engine.Execute("INSERT INTO users VALUES (1, 'Alice')")
engine.Execute("INSERT INTO logs VALUES ('User created')")

// Commit or rollback
if err != nil {
    engine.Rollback()
} else {
    engine.Commit()
}
```

### Virtual Table Implementation

1. Implement the `Module` interface
2. Implement the `VirtualTable` interface
3. Implement the `VirtualCursor` interface
4. Register with the virtual table registry
5. Use in SQL: `CREATE VIRTUAL TABLE ... USING module_name`

### Adding Built-in Functions

1. Implement function logic in `internal/functions`
2. Register with `Registry.RegisterScalar` or `RegisterAggregate`
3. Function becomes available in SQL expressions

## Related Documentation

- [QUICKSTART.md](QUICKSTART.md) - Getting started guide
- [LOCK_ORDERING.md](LOCK_ORDERING.md) - Concurrency and lock ordering details
- [SECURITY.md](SECURITY.md) - Security features and threat model
- [SUBQUERY_ARCHITECTURE.md](SUBQUERY_ARCHITECTURE.md) - Subquery implementation details
- [CTE_IMPLEMENTATION_SUMMARY.md](CTE_IMPLEMENTATION_SUMMARY.md) - CTE implementation
- [TRIGGER_INTEGRATION_REPORT.md](TRIGGER_INTEGRATION_REPORT.md) - Trigger system
- [INDEX.md](INDEX.md) - Documentation index

## References

- [SQLite Architecture](https://www.sqlite.org/arch.html)
- [VDBE Opcodes](https://www.sqlite.org/opcode.html)
- [SQLite File Format](https://www.sqlite.org/fileformat.html)
- [B-Trees](https://en.wikipedia.org/wiki/B-tree)
- [Database Internals (Book by Alex Petrov)](http://www.databass.dev/)
- [Write-Ahead Logging](https://www.sqlite.org/wal.html)

## License

This project is in the public domain (SQLite License).
