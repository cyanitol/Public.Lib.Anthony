# Anthony SQLite - Architecture Overview

This document describes the high-level architecture of the Anthony pure Go SQLite implementation.

## Design Philosophy

Anthony is a from-scratch implementation of SQLite in pure Go, following these principles:

1. **Pure Go**: No CGO dependencies, fully portable
2. **SQLite Compatible**: Follows SQLite file format and SQL dialect
3. **Clean Architecture**: Layered design with clear separation of concerns
4. **Production Quality**: Comprehensive testing, error handling, and documentation
5. **Public Domain**: Same license as SQLite

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

### 2. Query Processing Layer

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

### 3. Execution Engine (`internal/vdbe`)

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

### 4. Storage Layer

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

### 5. Supporting Subsystems

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

## Data Flow

### Query Execution (SELECT)

1. **Parse**: SQL text → AST (Abstract Syntax Tree)
2. **Plan**: AST → Query Plan (optimized)
3. **Compile**: Query Plan → VDBE bytecode
4. **Execute**: VDBE bytecode runs, produces rows
5. **Return**: Rows returned to application via driver

Example:
```sql
SELECT name, age FROM users WHERE age > 21 ORDER BY name;
```

Flow:
```
SQL Text
  ↓ Parser
SelectStmt AST
  ↓ Planner
Query Plan (index scan on age, sort by name)
  ↓ SQL Compiler
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
  ↓ VDBE Execution
Rows: [("Alice", 22), ("Bob", 25), ...]
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

## Performance Considerations

### Optimization Strategies

1. **Indexing**: Automatic index use for WHERE clauses
2. **Query Planning**: Cost-based optimizer selects best plan
3. **Page Caching**: Frequently-used pages kept in memory
4. **Write-Ahead Logging**: Better concurrency, faster writes
5. **Connection Pooling**: Reuse connections (via database/sql)

### Bottlenecks

- **Disk I/O**: Mitigated by page cache and WAL
- **Lock Contention**: Mitigated by reader-writer locks
- **Memory Allocation**: Minimized in hot paths
- **Expression Evaluation**: Compiled to efficient bytecode

## Testing Strategy

### Unit Tests
- Each package has comprehensive unit tests
- Test coverage > 80% for critical paths
- Edge cases and error conditions covered

### Integration Tests
- End-to-end query execution tests
- Transaction isolation tests
- Constraint enforcement tests

### Compatibility Tests
- SQLite test suite adaptation (where applicable)
- File format compatibility verification

## Future Directions

### Short Term
- Complete FOREIGN KEY support
- Full-text search (FTS5)
- Performance optimization

### Medium Term
- Concurrent query execution
- Query result caching
- Better statistics for query planner

### Long Term
- Distributed/replicated databases
- Pluggable storage engines
- Custom function modules

## References

- [SQLite Architecture](https://www.sqlite.org/arch.html)
- [VDBE Opcodes](https://www.sqlite.org/opcode.html)
- [B-Trees](https://en.wikipedia.org/wiki/B-tree)
- [Database Internals (Book by Alex Petrov)](http://www.databass.dev/)

## License

This project is in the public domain (SQLite License).
